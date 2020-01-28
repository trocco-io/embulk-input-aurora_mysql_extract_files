Embulk::JavaPlugin.register_input(
  "aurora_mysql_extract_files", "org.embulk.input.aurora_mysql_extract_files.AuroraMySQLExtractFilesFileInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
