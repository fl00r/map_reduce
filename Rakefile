# encoding: utf-8
require "bundler/gem_tasks"
require 'rake/testtask'

task :default => :spec

Rake::TestTask.new(:spec) do |t|
  t.libs << 'spec'
  t.pattern = 'spec/**/*_spec.rb'
  t.verbose = false
end