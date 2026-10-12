/// Identifier for the first Coral TypeScript Profile.
pub const TYPESCRIPT_PROFILE_ID: &str = "typescript-6.0.3-es2025-v1";

/// The one TypeScript compiler profile accepted by this Coral build.
pub const TYPESCRIPT_PROFILE: TypeScriptProfile = TypeScriptProfile {
    id: TYPESCRIPT_PROFILE_ID,
    compiler_version: "6.0.3",
    javascript_target: "ES2025",
    declaration_libraries: &["lib.es2025.d.ts"],
};

/// Pinned authoring and JavaScript-emission profile.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TypeScriptProfile {
    id: &'static str,
    compiler_version: &'static str,
    javascript_target: &'static str,
    declaration_libraries: &'static [&'static str],
}

impl TypeScriptProfile {
    /// Returns the stable profile identifier stored with compiled Programs.
    #[must_use]
    pub const fn id(self) -> &'static str {
        self.id
    }

    /// Returns the exact packaged official TypeScript compiler version.
    #[must_use]
    pub const fn compiler_version(self) -> &'static str {
        self.compiler_version
    }

    /// Returns the ECMAScript target emitted for embedded V8.
    #[must_use]
    pub const fn javascript_target(self) -> &'static str {
        self.javascript_target
    }

    /// Returns the ECMAScript declaration libraries selected for authoring.
    #[must_use]
    pub const fn declaration_libraries(self) -> &'static [&'static str] {
        self.declaration_libraries
    }
}
