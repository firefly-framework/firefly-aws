#  Copyright (c) 2020 JD Williams
#
#  This file is part of Firefly, a Python SOA framework built by JD Williams. Firefly is free software; you can
#  redistribute it and/or modify it under the terms of the GNU General Public License as published by the
#  Free Software Foundation; either version 3 of the License, or (at your option) any later version.
#
#  Firefly is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the
#  implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General
#  Public License for more details. You should have received a copy of the GNU Lesser General Public
#  License along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
#  You should have received a copy of the GNU General Public License along with Firefly. If not, see
#  <http://www.gnu.org/licenses/>.

from __future__ import annotations

import os

import boto3
import firefly_di as di

import firefly as ff

import firefly_aws.infrastructure as infra
import firefly_aws.domain as domain


class Container(di.Container):
    # AWS Services
    s3_client = lambda self: boto3.client('s3')
    sns_client = lambda self: boto3.client('sns')
    cloudformation_client = lambda self: boto3.client('cloudformation')
    lambda_client = lambda self: boto3.client('lambda')
    sns_client = lambda self: boto3.client('sns')
    sqs_client = lambda self: boto3.client('sqs')
    rds_data_client = lambda self: boto3.client('rds-data')
    ddb_client = lambda self: boto3.client('dynamodb')

    data_api: infra.DataApi = infra.DataApi
    s3_service: infra.BotoS3Service = infra.BotoS3Service
    lambda_executor: domain.LambdaExecutor = domain.LambdaExecutor
    jwt_decoder: domain.JwtDecoder = infra.CognitoJwtDecoder
    mutex: infra.DdbMutex = infra.DdbMutex
    file_system: ff.FileSystem = infra.S3FileSystem


if os.environ['FF_ENVIRONMENT'] != 'local':
    Container.message_transport = infra.BotoMessageTransport
    Container.__annotations__['message_transport'] = ff.MessageTransport
