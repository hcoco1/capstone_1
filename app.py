#!/usr/bin/env python3
import os

import aws_cdk as cdk

from capstone_1.capstone_1_stack import Capstone1Stack

env = cdk.Environment(account=os.getenv('CDK_DEFAULT_ACCOUNT'), region=os.getenv('CDK_DEFAULT_REGION'))


app = cdk.App()

stack1 = Capstone1Stack(app, "Capstone1Stack",)

app.synth()
