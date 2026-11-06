# Agent entry point

Read [AGENT.md](./AGENT.md) before changing this repository. It is the canonical working contract.
Area designs under `docs/src/` define their acceptance criteria; report observed verification
results and outstanding gates without treating design acceptance as implementation acceptance.

Every agent session, including resumed work, must follow the
[publishability rules](./AGENT.md#publishability) before each commit, amend, and push:

* Use `shuming.li <ming.moriarty@gmail.com>` for both Git author and committer; verify the
  stored identities after committing. Never use an Alibaba/corporate identity.
* Review staged content and outgoing commits for real credentials and non-public
  Alibaba/ODPS/MaxCompute material, including internal APIs, domains, and source code.
  Publicly accessible references and public APIs are allowed with verifiable public sources.
* Report findings without exposing suspected secret values. Follow the complete checklist in
  `AGENT.md`; a keyword scan alone does not satisfy it.
