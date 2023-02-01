# Create a file named: *configs.yml* in *this* directory (ex: config/configs.yml) that follows this format:

```
prisma_cloud:
  username: ""
  password: ""
  customer_name: ""
  api_base: "api.prismacloud.io"
  filename: ""
  rql: "config from cloud.resource where api.name = 'aws-ec2-describe-instances' addcolumn instanceId vpcId"
  utc: True #(True/False) sets time stamp format to include time zone (False) or be UTC time (True)
```

# Add your tenant credentials
The "username" should be an access key ID and the "password" should be a secret key. Always use access keys with expiration dates for scripts such as this one.

The "api_base" should be your tenants base url but replace "app" with "api". Do not include "https://"

# Update the "rql" field with the RQL expression you want to run