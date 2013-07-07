-type metadata_key() :: term().
-type metadata_value() :: term().

-record(metadata_v0, {
          values :: [metadata_value()],
          vclock :: vclock:vclock()
         }).
-type metadata() :: #metadata_v0{}.

