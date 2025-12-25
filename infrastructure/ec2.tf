# VPC + Subnet (single AZ)
resource "aws_vpc" "ec2_vpc" {
cidr_block = "10.0.0.0/16"
tags = { Name = "nginx-ec2-vpc"
Environment = "prd" }
}

data "aws_availability_zones" "available_zones" {}

resource "aws_subnet" "ec2_vpc_subnet" {
vpc_id = aws_vpc.ec2_vpc.id
cidr_block = "10.0.1.0/24"
availability_zone = data.aws_availability_zones.available_zones.names[0]
tags = { Name = "nginx-ec2-vpc-subnet"
Environment = "prd" }
}

resource "aws_internet_gateway" "ec2_gw" {
vpc_id = aws_vpc.ec2_vpc.id
}

resource "aws_route_table" "rt" {
vpc_id = aws_vpc.ec2_vpc.id
route {
cidr_block = "0.0.0.0/0"
gateway_id = aws_internet_gateway.ec2_gw.id
}
}

resource "aws_route_table_association" "rta" {
subnet_id = aws_subnet.ec2_vpc_subnet.id
route_table_id = aws_route_table.rt.id
}

resource "aws_security_group" "nginx_proxy_sg" {
    name = "nginx-proxy-sg"
    description = "Nginx proxy security group"
    vpc_id = aws_vpc.ec2_vpc.id

    tags = { Name = "nginx-proxy-security-group"
    Environment = "prd" }
}

resource "aws_vpc_security_group_ingress_rule" "allow_http_inbound" {
    security_group_id = aws_security_group.nginx_proxy_sg.id
    from_port = 80
    to_port = 80
    ip_protocol = "tcp"
    cidr_ipv4 = "0.0.0.0/0"
}

resource "aws_vpc_security_group_ingress_rule" "allow_https_inbound" {
    security_group_id = aws_security_group.nginx_proxy_sg.id
    from_port = 443
    to_port = 443
    ip_protocol = "tcp"
    cidr_ipv4 = "0.0.0.0/0"
}

resource "aws_vpc_security_group_egress_rule" "allow_all_egress" {
  security_group_id = aws_security_group.nginx_proxy_sg.id
  from_port = 0
  ip_protocol = "-1"
  cidr_ipv4 = "0.0.0.0/0"
}

resource "aws_security_group" "internal_proxy_sg" {
    name = "internal-proxy-sg"
    description = "Proxy for internal access"
    vpc_id = aws_vpc.ec2_vpc.id

    tags = { Name = "internal-proxy-security-group"
    Environment = "prd" }
}

resource "aws_vpc_security_group_ingress_rule" "allow_internal_all_inbound" {
    security_group_id = aws_security_group.internal_proxy_sg.id
    from_port = 0
    to_port = 0
    ip_protocol = "-1"
    cidr_ipv4 = "0.0.0.0/0"
}

resource "aws_vpc_security_group_egress_rule" "allow_internal_all_egress" {
    security_group_id = aws_security_group.internal_proxy_sg.id
    from_port = 0
    to_port = 0
    ip_protocol = "-1"
    cidr_ipv4 = "0.0.0.0/0"
}

data "aws_iam_policy_document" "ec2_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "ec2_role" {
  name_prefix = "investment_analytics-ec2-role-"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume_role.json
}

resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.ec2_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMFullAccess"
}

resource "aws_iam_instance_profile" "ec2_profile" {
  name_prefix = "investment-analytics-profile-"
  role = aws_iam_role.ec2_role.name
}

data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical
  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }
}

resource "aws_instance" "host" {
ami                         = data.aws_ami.ubuntu.id
instance_type               = var.instance_type
subnet_id                   = aws_subnet.ec2_vpc_subnet.id
vpc_security_group_ids      = [aws_security_group.nginx_proxy_sg.id, aws_security_group.internal_proxy_sg.id]
iam_instance_profile        = aws_iam_instance_profile.ec2_profile.name
associate_public_ip_address = true

# Initial Setup Script
user_data = file("./initial_script.sh")

root_block_device {
volume_size = 80
volume_type = "gp3"     
delete_on_termination = true
}

tags = {
  Name = "investment-analytics-data-warehouse-ec2-host"
}
}

# Allocate Elastic IP
resource "aws_eip" "ec2_eip" {
  domain = "vpc"
  tags = {
    Name = "investment-analytics-ec2-eip"
  }
}

# Attach Elastic IP to EC2 instance
resource "aws_eip_association" "ec2_eip_assoc" {
  instance_id   = aws_instance.host.id
  allocation_id = aws_eip.ec2_eip.id
}

output "ssm_instance_id" {
  value = aws_instance.host.id
}

output "elastic_ip" {
  value = aws_eip.ec2_eip.public_ip
}

















