# frozen_string_literal: true

# Test against multiple Rails/ActiveRecord versions
# Usage:
#   bundle exec appraisal install    # Generate gemfiles
#   bundle exec appraisal rspec      # Run specs against all versions
#   bundle exec appraisal rails-8.1 rspec  # Run against specific version

appraise 'rails-8.0' do
  gem 'activerecord', '~> 8.0.0'
end

appraise 'rails-8.1' do
  gem 'activerecord', '~> 8.1.0'
end

# Rails 8.2 is not released yet. Rename this to rails-8.2 with '~> 8.2.0' once it ships.
appraise 'rails-main' do
  gem 'activerecord', github: 'rails/rails', branch: 'main'
end
