# encoding: UTF-8
lib = File.expand_path('../lib/', __FILE__)
$:.unshift lib unless $:.include?(lib)
 
Gem::Specification.new do |s|
  s.name        = 'log2carbon'
  s.version     = '0.2.0'
  s.platform    = Gem::Platform::RUBY
  s.authors     = ["Josep M. Pujol"]
  s.email       = 'josep@3scale.net'
  s.homepage    = 'http://www.3scale.net'
  s.summary     = ''
  s.description = ''

  s.required_rubygems_version = ">= 1.3.7"

  s.add_dependency 'daemons', '1.1.9'

  s.files = Dir.glob('{lib,bin}/**/*')
  s.files << 'README.md'
  s.files << 'Rakefile'

  s.executables  = ['log2carbon']
  s.require_path = 'lib'
end
