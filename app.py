#!/usr/bin/env python3

from aws_cdk import App, Tags

from lib.convert.convert_stack import CallRecordStack


app = App()
callrecording = CallRecordStack(app, "callrecording-compression", description='Connect Audio compression service')
Tags.of(callrecording).add("project", "callrecording-batch-compression")
app.synth()
