Embulk::JavaPlugin.register_input(
  "swift", "org.embulk.input.swift.SwiftFileInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
