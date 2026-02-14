macro_rules! table {
    (
        @implement
        #[doc = $doc:expr]
        $name:ident, $key:ty, $value:ty, $dual:expr, $fixed:expr
    ) => {
        #[doc = $doc]
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub struct $name;

        impl crate::tables::Table for $name {
            const NAME: &'static str = stringify!($name);
            const FIXED_VAL_SIZE: Option<usize> = $fixed;
            const DUAL_KEY_SIZE: Option<usize> = $dual;
            type Key = $key;
            type Value = $value;
        }

    };

    // Single key
    (
        #[doc = $doc:expr]
        $name:ident<$key:ty => $value:ty>
    ) => {
        table!(@implement
            #[doc = $doc]
            $name,
            $key,
            $value,
            None,
            None
        );

        impl crate::tables::SingleKey for $name {}
    };

    // Dual key
    (
        #[doc = $doc:expr]
        $name:ident<$key:ty => $subkey:ty => $value:ty>
    ) => {
        table!(@implement
            #[doc = $doc]
            $name,
            $key,
            $value,
            Some(<$subkey as crate::ser::KeySer>::SIZE),
            None
        );

        impl crate::tables::DualKey for $name {
            type Key2 = $subkey;
        }
    };

    // Dual key with fixed size
    (
        #[doc = $doc:expr]
        $name:ident<$key:ty => $subkey:ty => $value:ty> is $fixed:expr
    ) => {
        table!(@implement
            #[doc = $doc]
            $name,
            $key,
            $value,
            Some(<$subkey as crate::ser::KeySer>::SIZE),
            Some($fixed)
        );

        impl crate::tables::DualKey for $name {
            type Key2 = $subkey;
        }
    };

    // Dual key with fixed size and mode
    (
        #[doc = $doc:expr]
        $name:ident<$key:ty => $subkey:ty => $value:ty> is $fixed:expr, $mode:ident
    ) => {
        table!(@implement
            #[doc = $doc]
            $name,
            $key,
            $value,
            Some(<$subkey as crate::ser::KeySer>::SIZE),
            Some($fixed)
        );

        impl crate::tables::DualKey for $name {
            type Key2 = $subkey;
            const TABLE_MODE: crate::tables::DualTableMode =
                crate::tables::DualTableMode::$mode;
        }
    };
}
