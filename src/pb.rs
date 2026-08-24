//! Generated protobuf types for `uniswap.v4.v1`.
//!
//! The nesting mirrors the proto package so that the generated file, which
//! prost names after the package (`uniswap.v4.v1.rs`), lands at the path every
//! module imports: `crate::pb::uniswap::v4::v1`.

pub mod uniswap {
    pub mod v4 {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/uniswap.v4.v1.rs"));
        }
    }
}
