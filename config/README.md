# Create a file named: *configs.yml* that follows this format:

```
prisma_cloud:
  username: ""
  password: ""
  customer_name: ""
  api_base: "api.ca.prismacloud.io"
  filename: ""
  rql: "config from cloud.resource where api.name = 'aws-ec2-describe-instances' addcolumn instanceId vpcId"
```

# Add your tenant credentials and proper API base URL.

# Update the RQL field with the RQL you want to run