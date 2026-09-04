// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, StringArray, StringBuilder, StringViewArray, StringViewBuilder,
};
use arrow::csv::ReaderBuilder;
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::errors::Result;
use parquet::file::properties::WriterProperties;

pub fn convert(
    input: impl AsRef<Path>,
    output: impl AsRef<Path>,
    schema: SchemaRef,
    delimiter: u8,
) -> Result<()> {
    let input = File::open(input)?;
    let reader = ReaderBuilder::new(schema)
        .with_delimiter(delimiter)
        .with_escape(b'\\')
        .with_quote(b'"')
        .build(input)?;

    let output = File::create(output)?;
    let properties = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .build();
    let mut writer = ArrowWriter::try_new(output, reader.schema(), Some(properties))?;

    for batch in reader {
        writer.write(&replace_empty_strings_with_nulls(batch?)?)?;
    }
    writer.close()?;
    Ok(())
}

fn replace_empty_strings_with_nulls(batch: RecordBatch) -> arrow::error::Result<RecordBatch> {
    let columns = batch
        .columns()
        .iter()
        .zip(batch.schema().fields())
        .map(|(column, field)| match field.data_type() {
            DataType::Utf8 if field.is_nullable() => {
                let strings = column
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("Utf8 columns must be StringArray");
                let mut builder = StringBuilder::new();
                for index in 0..strings.len() {
                    if strings.is_null(index) || strings.value(index).is_empty() {
                        builder.append_null();
                    } else {
                        builder.append_value(strings.value(index));
                    }
                }
                Arc::new(builder.finish()) as ArrayRef
            }
            DataType::Utf8View if field.is_nullable() => {
                let strings = column
                    .as_any()
                    .downcast_ref::<StringViewArray>()
                    .expect("Utf8View columns must be StringViewArray");
                let mut builder = StringViewBuilder::new();
                for index in 0..strings.len() {
                    if strings.is_null(index) || strings.value(index).is_empty() {
                        builder.append_null();
                    } else {
                        builder.append_value(strings.value(index));
                    }
                }
                Arc::new(builder.finish()) as ArrayRef
            }
            _ => Arc::clone(column),
        })
        .collect();

    RecordBatch::try_new(batch.schema(), columns)
}
