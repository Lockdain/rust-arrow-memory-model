use std::error::Error;
use std::fs::File;
use std::mem::take;
use std::sync::Arc;
use arrow::array::{Array, ArrayRef, Float64Array, Int32Array, RecordBatch, StringArray, UInt32Array};
use arrow::compute::sort_to_indices;
use arrow::csv;
use arrow::csv::reader::Format;
use arrow::csv::ReaderBuilder;
use arrow::datatypes::{DataType, Field, Int16Type, Schema};
use arrow::error::Result as ArrowResult;

fn main() {
    //let primitive_array = array();
    //println!("{:?}", primitive_array);
    //complex_type().expect("Error is happened");
    //reader_builder_example().expect("Error is happened");
    sort_by_category(reader_builder());
}

fn array() -> Int32Array {
    let array = Int32Array::from(vec![Some(1), None, Some(2), None, Some(6)]);
    assert_eq!(array.len(), 5);

    array
}

/*Массивы - низкоуровневая основа работы с памятью, для работы
с dataset массивы "оборачиваются" дополнительными метаданными в
виде Field, Schema и RecordBatch.
Field - метаданные колонки
Schema - группировка векторов Field
RecordBatch - Schema + Fields
*/

fn complex_type() -> ArrowResult<()> {
    
    let schema = Schema::new(vec![
        Field::new("string", DataType::Utf8, false),
        Field::new("int", DataType::Int32, false),
        Field::new("float", DataType::Float64, false),
    ]);

    let string_array = StringArray::from(vec!["one", "two", "three", "four", "five"]);
    let int32_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    let float64_array = Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0]);

    /// RecordBatch cодержит ссылки на на схему и примитивы, дает zero-copy внутри одного узла
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(string_array),
            Arc::new(int32_array),
            Arc::new(float64_array)
        ]
    )?;

    println!("{:?}", batch);

    Ok(())
}

fn reader_builder_example() -> ArrowResult<()> {
    let mut file = File::open("shopping_trends.csv")?;

    let (schema, _) = Format::default().infer_schema(&mut file, Some(100))?;
    let mut csv_builder = ReaderBuilder::new(Arc::new(schema)).build(file)?;

    let batch = csv_builder.next().unwrap()?;

    println!("{:?}", batch);

    Ok(())

}

fn sort_by_category(batch: Result<RecordBatch, Box<dyn Error>>) -> UInt32Array {
    //Получаем массив отсортированных индексов
    let indices = sort_to_indices(batch
                                      .unwrap()
                                      .column(5), None, None);

    println!("{:?}", indices);
    indices.unwrap()
}

fn reader_builder() -> Result<RecordBatch, Box<dyn Error>> {
    let mut file = File::open("shopping_trends.csv")?;

    let (schema, _) = Format::default().infer_schema(&mut file, Some(100))?;
    let mut csv_builder = ReaderBuilder::new(Arc::new(schema)).build(file)?;

    let batch = csv_builder.next().unwrap()?;

    println!("{:?}", batch);

    Ok(batch)

}