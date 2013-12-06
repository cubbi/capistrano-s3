require 'aws/s3'
require 'mime/types'
require 'fileutils'
require 'zlib'
require 'stringio'

module Capistrano
  module S3
    module Publisher

      LAST_PUBLISHED_FILE = '.last_published'

      GZIPABLE = %w[text/css text/html application/javascript] 

      def self.publish!(s3_endpoint, key, secret, bucket, source, cloudfront, extra_options)
        s3 = self.establish_s3_client_connection!(s3_endpoint, key, secret)
        
        published = []
        
        self.files(source).each do |file|
          if !File.directory?(file)
            next if self.published?(file)

            path = self.base_file_path(source, file)
            path.gsub!(/^\//, "") # Remove preceding slash for S3
            self.put_object(s3, bucket, path, file, extra_options)
            
            published << "/#{path}" # Asume that file was uploaded, add it to published list            
          end
        end
        FileUtils.touch(LAST_PUBLISHED_FILE)
        
        unless cloudfront.empty?
          cf = self.establish_cloudfront_client_connection!(key, secret)          

          options = {
            :distribution_id => cloudfront,
            :invalidation_batch => { :caller_reference => "#{cloudfront}-capistrano-s3-#{Time.now.to_i}" }
          }

          paths = { :quantity => published.count, :items => published}          
          options[:invalidation_batch].merge!(:paths => paths)
          puts options

          cf.client.create_invalidation(options)          
        end
                
      end

      def self.clear!(s3_endpoint, key, secret, bucket)
        s3 = self.establish_s3_connection!(s3_endpoint, key, secret)
        s3.buckets[bucket].clear!

        FileUtils.rm(LAST_PUBLISHED_FILE)
      end

      private

      # Establishes the connection to Amazon S3
      def self.establish_connection!(klass, s3_endpoint, key, secret)
        # Send logging to STDOUT
        AWS.config(:logger => ::Logger.new(STDOUT))
        klass.new(
        :s3_endpoint => s3_endpoint,
        :access_key_id => key,
        :secret_access_key => secret
        )
      end

      def self.establish_s3_client_connection!(s3_endpoint, key, secret)
        self.establish_connection!(AWS::S3::Client, s3_endpoint, key, secret)
      end

      def self.establish_cloudfront_client_connection!(key, secret)
        AWS::CloudFront.new(
          :access_key_id => key,
          :secret_access_key => secret
        )
      end

      def self.establish_s3_connection!(s3_endpoint, key, secret)
        self.establish_connection!(AWS::S3, s3_endpoint, key, secret)
      end

      def self.base_file_path(root, file)
        file.gsub(root, "")
      end

      def self.files(deployment_path)
        Dir.glob("#{deployment_path}/**/*")
      end

      def self.published?(file)
        return false unless File.exists? LAST_PUBLISHED_FILE
        File.mtime(file) < File.mtime(LAST_PUBLISHED_FILE)
      end

      def self.put_object(s3, bucket, path, file, extra_options)
        options = {
          :bucket_name => bucket,
          :key => path,
          :data => open(file),
          :acl => :public_read,
        }

        content_type = self.build_content_type_hash(file)

        if GZIPABLE.include?(content_type[:content_type])  

          gzipedFile = StringIO.new("")
          compressor = Zlib::GzipWriter.new(gzipedFile)
          compressor.write (options[:data]).read
          compressor.close

          memoryFile = StringIO.new gzipedFile.string
          options.merge!({:data => memoryFile})     
          options.merge!({:content_encoding => "gzip"})         
        end

        options.merge!(content_type)
        options.merge!(self.build_redirect_hash(path, extra_options[:redirect]))
        options.merge!(extra_options[:write]) if extra_options[:write]

        s3.put_object(options)
      end

      def self.build_content_type_hash(file)
        type = MIME::Types.type_for(File.basename(file))
        return {} unless type && !type.empty?

        { :content_type => type.first.content_type }
      end

      def self.build_redirect_hash(path, redirect_options)
        return {} unless redirect_options && redirect_options[path]
        { :website_redirect_location => redirect_options[path] }
      end
    end
  end
end
