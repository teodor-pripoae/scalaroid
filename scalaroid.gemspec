# coding: UTF-8

Gem::Specification.new do |s|
  s.name              = "scalaroid"
  s.version           = "0.0.1"
  s.platform          = Gem::Platform::RUBY
  s.authors           = ["Teodor Pripoae"]
  s.email             = ["toni@kuende.com"]
  s.homepage          = "https://github.com/teodor-pripoae/scalaroid"
  s.summary           = "Ruby bindings for Scalaris, inspired from original scalaris gem"
  s.description       = "Ruby bindings for Scalaris, inspired from original scalaris gem"

  s.required_rubygems_version = ">= 1.3.6"

  # If you have runtime dependencies, add them here
  s.add_runtime_dependency "json", ">= 1.4.0"

  # If you have development dependencies, add them here
  # s.add_development_dependency "another", "= 0.9"

  # The list of files to be contained in the gem
  s.files         = ["lib/scalaroid.rb", "lib/scalaroid/version.rb"]
  s.bindir        = 'bin'
  s.executables   = Dir["scalaroid"]
  # s.extensions    = Dir["ext/extconf.rb"]

  # s.require_path = 'lib'

  # For C extensions
  # s.extensions = "ext/extconf.rb"
end
