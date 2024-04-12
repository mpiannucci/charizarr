/// This is a port of https://github.com/scalableminds/zarrita/blob/async/zarrita/indexing.py

use std::ops::Range;

#[derive(Debug, Clone)]
pub struct ChunkIndexProjection {
    chunk_index: usize,
    chunk_sel: Range<usize>,
    out_sel: Range<usize>,
}

#[derive(Debug, Clone)]
pub struct SliceDimIndexIterator {
    sel: Range<usize>,
    dim_len: usize,
    chunk_len: usize,
    nitems: usize,
    current_chunk_index: usize,
}

impl SliceDimIndexIterator {
    pub fn new(dim_len: usize, chunk_len: usize, sel: Range<usize>) -> Self {
        let nitems = (sel.end - sel.start).max(0);
        let current_chunk_index = sel.start / chunk_len;
        let end_chunk_index = (((sel.end + chunk_len) as f64) / chunk_len as f64).ceil() as usize;
        Self {
            sel,
            dim_len,
            chunk_len,
            nitems,
            current_chunk_index,
        }
    }
}

impl Iterator for SliceDimIndexIterator {
    type Item = ChunkIndexProjection;

    fn next(&mut self) -> Option<Self::Item> {
        let dim_offset = self.current_chunk_index * self.chunk_len;
        if dim_offset >= self.sel.end {
            return None;
        }

        let dim_limit = ((self.current_chunk_index + 1) * self.chunk_len).min(self.dim_len);

        // determine chunk length, accounting for trailing chunk
        let dim_chunk_len = dim_limit - dim_offset;
        let (dim_chunk_sel_start, dim_out_offset) = if self.sel.start < dim_offset {
            let dim_out_offset = dim_offset - self.sel.start;
            (0usize, dim_out_offset)
        } else {
            let dim_chunk_sel_start = self.sel.start - dim_offset;
            (dim_chunk_sel_start, 0usize)
        };

        let dim_chunk_sel_stop = if self.sel.end > dim_limit {
            dim_chunk_len
        } else {
            self.sel.end - dim_offset
        };

        let chunk_sel = dim_chunk_sel_start..dim_chunk_sel_stop;
        let chunk_nitems = dim_chunk_sel_stop - dim_chunk_sel_start;
        let out_sel = dim_out_offset..dim_out_offset + chunk_nitems;

        let chunk_index = self.current_chunk_index;
        self.current_chunk_index += 1;

        Some(ChunkIndexProjection {
            chunk_index,
            chunk_sel,
            out_sel,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slice_dim_indexer() {
        // Assuming dimensions of 10, chunk length of 2 and selection of 1..4
        // 0 [1 | 2 3] | 4 5 | 6 7 | 8 9
        let indexer = SliceDimIndexIterator::new(10, 2, 1..4);
        assert_eq!(indexer.dim_len, 10);
        assert_eq!(indexer.chunk_len, 2);
        assert_eq!(indexer.nitems, 3);
        assert_eq!(indexer.sel, 1..4);
        assert_eq!(indexer.current_chunk_index, 0);

        let chunks: Vec<_> = indexer.collect();
        assert_eq!(chunks.len(), 2);

        // 0 [1] |
        let first_chunk = &chunks[0];
        assert_eq!(first_chunk.chunk_index, 0);
        assert_eq!(first_chunk.chunk_sel, 1..2);
        assert_eq!(first_chunk.out_sel, 0..1);

        // 0 1 | [2 3] |
        let second_chunk = &chunks[1];
        assert_eq!(second_chunk.chunk_index, 1);
        assert_eq!(second_chunk.chunk_sel, 0..2);
        assert_eq!(second_chunk.out_sel, 1..3);
    }
}
