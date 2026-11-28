"""Compare two release builds on identical Q6 writes in separately seeded warehouses."""
import argparse
import csv
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import shutil
from statistics import median
import subprocess
import tempfile


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def income(rows):
    return sum((Decimal(r[5]) * Decimal(r[6]) for r in rows
                if '1994-01-01' <= r[10] < '1995-01-01'
                and Decimal('.07') <= Decimal(r[6]) <= Decimal('.09')
                and Decimal(r[4]) < 24), Decimal(0))


def insert_sql(rows):
    def literal(i, value):
        quoted = "'" + value.replace("'", "''") + "'"
        if i < 4:
            return str(int(value))
        if i < 8:
            return f'CAST({quoted} AS DECIMAL(15,2))'
        return 'DATE ' + quoted if 10 <= i <= 12 else quoted

    return 'INSERT INTO lineitem VALUES ' + ','.join(
        '(' + ','.join(literal(i, v) for i, v in enumerate(row)) + ')' for row in rows)


def prepare(out):
    repo = Path(__file__).resolve().parents[3]
    spec = repo / 'optd-incremental-sql/benchmarks/tpch'
    manifest = json.loads((spec / 'manifest.json').read_text())
    q = next(q['variants'][0] for q in manifest['queries'] if q['query_id'] == 6)
    t = next(t for t in manifest['tables'] if t['name'] == 'lineitem')
    source, query, schema = repo / t['csv'], spec / q['sql'], spec / 'schema.sql'
    for path, expected in [(source, t['sha256']), (query, q['sha256']), (schema, manifest['schema_sha256'])]:
        if digest(path) != expected:
            raise ValueError(f'fixture digest mismatch: {path}')
    with source.open() as stream:
        rows = list(csv.reader(stream, delimiter='|'))
    initial = income(rows)
    if initial != Decimal('90927.6243') or len(rows) != 6005:
        raise ValueError('unexpected Q6 fixture')
    ddl = 'CREATE TABLE LINEITEM (' + schema.read_text().split('CREATE TABLE LINEITEM (')[1]
    inserts = [insert_sql(rows[i:i+256]) for i in range(0, len(rows), 256)]
    (out / 'seed.sql').write_text(ddl + '\nBEGIN;\n' + ';\n'.join(inserts) + ';\nCOMMIT;\n')
    matching = next(row for row in rows if income([row]))
    stages = []
    for n, selected in [(1, [matching]), (100, [matching] + rows[:99])]:
        new = [[str(1000000 + i)] + r[1:] for i, r in enumerate(selected)]
        stages.extend([
            dict(stage=f'insert_{n}', sql=insert_sql(new), expected=str(initial + income(new)), affected=n),
            dict(stage=f'delete_{n}', sql='DELETE FROM lineitem WHERE l_orderkey>=1000000',
                 expected=str(initial), affected=n)])
    work = dict(query=query.read_text(), initial=str(initial), stages=stages, cycles=7)
    (out / 'workload.json').write_text(json.dumps(work, indent=2))
    return work


def run(*args):
    return json.loads(subprocess.check_output(args, text=True))


def verify(record, expected):
    return (record['schema'] == [['revenue', 'Decimal128(38, 4)', True]]
            and len(record['rows']) == 1 and len(record['rows'][0]) == 1
            and Decimal(record['rows'][0][0]) == Decimal(expected))


def checkpoint_steps(root):
    head = json.loads((root / 'CURRENT').read_text())
    epoch_checkpoints = []
    while head:
        entry = json.loads((root / head['directory'] / head['manifest']['name']).read_text())
        state = entry.get('view_states', {}).get('q6_revenue')
        if state and state['checkpoint_epoch'] == entry['epoch']:
            epoch_checkpoints.append(entry['epoch'])
        head = entry.get('previous')
    bootstrap = min(epoch_checkpoints)
    return sorted(epoch - bootstrap for epoch in epoch_checkpoints if epoch > bootstrap)


def summarize(samples, stages, lanes):
    steps = range(4, 28)
    groups = {stage['stage']: [i for i in steps if i % 4 == j]
              for j, stage in enumerate(stages)}
    # The retained manifests independently confirm the 16th mutation checkpointed.
    groups['ordinary'] = [i for i in steps if i != 15]
    groups['checkpoint'] = [15]
    summary = {}
    for label, indices in groups.items():
        keys = [(trial, i) for trial in range(4) for i in indices]
        row = {'pairs': len(keys)}
        for version, mode in lanes:
            records = [samples[t, i, version, mode] for t, i in keys]
            row[f'{version}_{mode}'] = {
                metric + '_ms': median(r[metric + '_us'] for r in records) / 1000
                for metric in ('dml', 'read', 'total')
            }
        savings = [samples[t, i, 'old', 'incremental']['dml_us']
                   - samples[t, i, 'new', 'incremental']['dml_us'] for t, i in keys]
        row['paired_write_saving_ms'] = median(savings) / 1000
        summary[label] = row
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('output', type=Path, help='new output directory')
    for name in ['old-cli', 'new-cli', 'old-driver', 'new-driver']:
        parser.add_argument('--' + name, required=True, type=Path)
    args = parser.parse_args()
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    work = prepare(out)
    builds = {'old': (args.old_cli.resolve(), args.old_driver.resolve()),
              'new': (args.new_cli.resolve(), args.new_driver.resolve())}
    for name, (cli, _) in builds.items():
        run(cli, out / name, out / 'seed.sql')
    lanes = [('old', 'incremental'), ('new', 'incremental'), ('old', 'full'), ('new', 'full')]
    samples = {}
    for trial in range(4):
        # Each lane occupies each position once; adjacent lane order is balanced.
        for slot in [0, 1, 3, 2]:
            name, mode = lanes[(trial + slot) % 4]
            cli, driver = builds[name]
            with tempfile.TemporaryDirectory(dir=out) as tmp:
                root = Path(tmp) / 'warehouse'
                shutil.copytree(out / name, root)
                data = run(driver, root, out / 'workload.json', mode)
                replay = run(cli, root, '--replay')
                if mode == 'incremental' and checkpoint_steps(root) != [16]:
                    raise ValueError('checkpoint observations differ from grouping')
                if not replay['output_history_verified'] or not verify(data['initial'], work['initial']):
                    raise ValueError(f'{name}/{mode}: replay or initial result mismatch')
                if len(data['rounds']) != 28:
                    raise ValueError(f'{name}/{mode}: missing mutation rounds')
                for i, r in enumerate(data['rounds']):
                    stage = work['stages'][i % 4]
                    if (not verify(r, stage['expected']) or r['affected'] != stage['affected']
                            or r['stage'] != stage['stage']):
                        raise ValueError(f'{name}/{mode}: incorrect result at step {i+1}')
                    if i >= 4:
                        samples[trial, i, name, mode] = r
                (out / f'{trial}-{name}-{mode}.json').write_text(json.dumps(data))
    report = {
        'rows': summarize(samples, work['stages'], lanes),
        'units': 'ms', 'warmup_pairs': 16, 'measured_pairs': 96,
        'binaries': {name: [digest(path) for path in paths] for name, paths in builds.items()},
        'workload_sha256': digest(out / 'workload.json'),
    }
    (out / 'comparison.json').write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
