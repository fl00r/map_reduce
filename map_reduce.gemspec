# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'map_reduce/version'

Gem::Specification.new do |spec|
  spec.name          = "map_reduce"
  spec.version       = MapReduce::VERSION
  spec.authors       = ["Petr Yanovich"]
  spec.email         = ["fl00r@yandex.ru"]
  spec.description   = %q{Simple distributed Map Reduce Framework on Ruby}
  spec.summary       = %q{Simple distributed Map Reduce Framework on Ruby}
  spec.homepage      = ""
  spec.license       = "MIT"

  spec.files         = `git ls-files`.split($/)
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.3"
  spec.add_development_dependency "rake"
  spec.add_development_dependency "em-synchrony"

  spec.add_dependency "em-zmq-tp10"
end
