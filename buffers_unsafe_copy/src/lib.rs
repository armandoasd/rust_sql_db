
pub mod string {
    use std::ptr;
    pub fn unsafe_copy(string: &String)-> String {
        unsafe {
            from_buf_raw(string.as_ptr(), string.len())
        }
    }

    pub fn unsafe_copy_option_str(string_o: &Option<String>)-> Option<String> {
        unsafe {
            if let Some (string) = string_o {
                Some(from_buf_raw(string.as_ptr(), string.len()))
            } else {
                None
            }
        }
    }
    
    unsafe fn from_buf_raw(ptr: *const u8, elts: usize) -> String {
        let mut dst = String::with_capacity(elts);
    
        ptr::copy(ptr, dst.as_mut_ptr(), elts);
    
        dst.as_mut_vec().set_len(elts);
        dst
    }
}

pub mod vector {
    use std::ptr;
    pub fn unsafe_copy<T>(vec: &Vec<T>)-> Vec<T> {
        unsafe {
            from_buf_raw(vec.as_ptr(), vec.len())
        }
    }

    unsafe fn from_buf_raw<T>(ptr: *const T, elts: usize) -> Vec<T> {
        let mut dst = Vec::with_capacity(elts);
    
        // SAFETY: Our precondition ensures the source is aligned and valid,
        // and `Vec::with_capacity` ensures that we have usable space to write them.
        ptr::copy(ptr, dst.as_mut_ptr(), elts);
    
        // SAFETY: We created it with this much capacity earlier,
        // and the previous `copy` has initialized these elements.
        dst.set_len(elts);
        dst
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
