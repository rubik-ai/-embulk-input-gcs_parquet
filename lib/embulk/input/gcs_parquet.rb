Embulk::JavaPlugin.register_input(
  :gcs_parquet, "org.embulk.input.gcs.parquet.GcsParquetInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
