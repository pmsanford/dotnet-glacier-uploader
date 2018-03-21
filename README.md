# dotnet-glacier-uploader
Minimal multipart uploader for large files to Amazon Glacier

1. Install [dotnet SDK 2.1](https://www.microsoft.com/net/download)
2. Sign into your AWS account and [create a Glacier vault](https://console.aws.amazon.com/glacier/home)
3. Clone this repo
4. In the repo directory, run `dotnet run upload <aws access key> <aws secret key> <region_name> <glacier vault name> <file description> <file path>`

The file description is the only identifier for the archive in AWS. The vault name should obviously match the name from step 2. region should be the system name (for ex 'us-west-1')

This is designed for uploading large files, like tar.gz archives. I don't know how it'll work with smaller files.
