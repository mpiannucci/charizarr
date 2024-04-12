/// This is a port of https://github.com/scalableminds/zarrita/blob/async/zarrita/indexing.py
use std::ops::Range;

use itertools::{izip, Itertools, MultiProduct};

#[derive(Debug, Clone)]
pub struct ChunkIndexProjection {
    pub chunk_index: usize,
    pub chunk_sel: Range<usize>,
    pub out_sel: Range<usize>,
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

#[derive(Clone, Debug)]
pub struct ChunkProjection {
    pub chunk_coords: Vec<usize>,
    pub chunk_sel: Vec<Range<usize>>,
    pub out_sel: Vec<Range<usize>>,
}

#[derive(Debug, Clone)]
pub struct BasicIndexIterator {
    indexes: MultiProduct<SliceDimIndexIterator>,
    pub shape: Vec<usize>,
}

impl BasicIndexIterator {
    pub fn new(shape: Vec<usize>, chunk_shape: Vec<usize>, sel: Vec<Range<usize>>) -> Self {
        let indexes = izip!(shape, chunk_shape, sel)
            .map(|(dim_len, chunk_len, sel)| SliceDimIndexIterator::new(dim_len, chunk_len, sel))
            .collect::<Vec<_>>();
        let sel_shape = indexes.iter().map(|indexer| indexer.nitems).collect();

        let indexes = indexes.into_iter().multi_cartesian_product();

        Self {
            indexes,
            shape: sel_shape,
        }
    }
}

impl Iterator for BasicIndexIterator {
    type Item = ChunkProjection;

    fn next(&mut self) -> Option<Self::Item> {
        let chunk_indexes = self.indexes.next()?;

        let chunk_coords = chunk_indexes
            .iter()
            .map(|index| index.chunk_index)
            .collect();
        let chunk_sel = chunk_indexes
            .iter()
            .map(|index| index.chunk_sel.clone())
            .collect();
        let out_sel = chunk_indexes
            .iter()
            .map(|index| index.out_sel.clone())
            .collect();

        Some(ChunkProjection {
            chunk_coords,
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

    #[test]
    fn test_basic_index_iterator() {
        // Assuming shape of (6,2), chunk shape of (3, 2) and selection of [2..5, 1..2]
        //  0 1  2  | 3  4  5
        //  6 7 [8  | 9 10] 11
        let chunks = BasicIndexIterator::new(vec![6, 2], vec![3, 2], vec![2..5, 1..2]);
        assert_eq!(chunks.shape, vec![3, 1]);

        let chunks: Vec<_> = chunks.collect();
        assert_eq!(chunks.len(), 2);

        let first_chunk = &chunks[0];
        assert_eq!(first_chunk.chunk_coords, vec![0, 0]);
        assert_eq!(first_chunk.chunk_sel, vec![2..3, 1..2]);
        assert_eq!(first_chunk.out_sel, vec![0..1, 0..1]);

        let second_chunk = &chunks[1];
        assert_eq!(second_chunk.chunk_coords, vec![1, 0]);
        assert_eq!(second_chunk.chunk_sel, vec![0..2, 1..2]);
        assert_eq!(second_chunk.out_sel, vec![1..3, 0..1]);
    }
}
