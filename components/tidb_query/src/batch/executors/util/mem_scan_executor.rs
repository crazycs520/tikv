use crate::codec::batch::LazyBatchColumnVec;
use crate::Result;

use tipb::FieldType;

pub trait MemScanExecutor {
    fn schema(&self) -> &[FieldType];

    fn build_column_vec(&self, rows: usize) -> LazyBatchColumnVec;

    fn process_row(&mut self, columns: &mut LazyBatchColumnVec) -> Result<()>;
}
