# Generated by jeweler
# DO NOT EDIT THIS FILE DIRECTLY
# Instead, edit Jeweler::Tasks in Rakefile, and run 'rake gemspec'
# -*- encoding: utf-8 -*-
# stub: scalaroid 0.0.1 ruby lib

Gem::Specification.new do |s|
  s.name = "scalaroid"
  s.version = "0.0.1"

  s.required_rubygems_version = Gem::Requirement.new(">= 0") if s.respond_to? :required_rubygems_version=
  s.require_paths = ["lib"]
  s.authors = ["Teodor Pripoae"]
  s.date = "2014-07-07"
  s.description = "Ruby bindings for Scalaris forked from official Scalaris bindings"
  s.email = "teodor.pripoae@gmail.com"
  s.executables = ["scalaroid"]
  s.extra_rdoc_files = [
    "LICENSE.txt"
  ]
  s.files = [
    ".document",
    "Gemfile",
    "Gemfile.lock",
    "LICENSE.txt",
    "Rakefile",
    "bin/scalaroid",
    "lib/scalaroid.rb",
    "lib/scalaroid/json_connection.rb",
    "lib/scalaroid/version.rb",
    "scalaroid.gemspec",
    "test/replicated_dht_test.rb",
    "test/test_helper.rb",
    "test/test_pub_sub.rb",
    "test/transaction_single_op_test.rb",
    "test/transaction_test.rb"
  ]
  s.homepage = "http://github.com/teodor-pripoae/scalaroid"
  s.licenses = ["Apache-2.0"]
  s.rubygems_version = "2.2.2"
  s.summary = "Ruby bindings for Scalaris"

  if s.respond_to? :specification_version then
    s.specification_version = 4

    if Gem::Version.new(Gem::VERSION) >= Gem::Version.new('1.2.0') then
      s.add_runtime_dependency(%q<scalaroid>, [">= 0"])
      s.add_runtime_dependency(%q<json>, [">= 0"])
      s.add_development_dependency(%q<rake>, ["~> 10.0"])
      s.add_development_dependency(%q<minitest>, ["~> 5.3"])
      s.add_development_dependency(%q<pry>, ["= 0.9.12.6"])
      s.add_development_dependency(%q<bundler>, ["~> 1.0"])
    else
      s.add_dependency(%q<scalaroid>, [">= 0"])
      s.add_dependency(%q<json>, [">= 0"])
      s.add_dependency(%q<rake>, ["~> 10.0"])
      s.add_dependency(%q<minitest>, ["~> 5.3"])
      s.add_dependency(%q<pry>, ["= 0.9.12.6"])
      s.add_dependency(%q<bundler>, ["~> 1.0"])
    end
  else
    s.add_dependency(%q<scalaroid>, [">= 0"])
    s.add_dependency(%q<json>, [">= 0"])
    s.add_dependency(%q<rake>, ["~> 10.0"])
    s.add_dependency(%q<minitest>, ["~> 5.3"])
    s.add_dependency(%q<pry>, ["= 0.9.12.6"])
    s.add_dependency(%q<bundler>, ["~> 1.0"])
  end
end
