fn main() {
    let protoc = protoc_bin_vendored::protoc_bin_path().expect("find vendored protoc");
    unsafe {
        std::env::set_var("PROTOC", protoc);
    }

    tonic_build::configure()
        .build_server(false)
        .compile_protos(&["proto/tetragon/api.proto"], &["proto"])
        .expect("compile tetragon protos");
}
