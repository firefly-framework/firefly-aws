from __future__ import annotations

import os
import shutil

import firefly as ff
import yaml


class PackageAndDeployCode(ff.DomainService):
    _configuration: ff.Configuration = None
    _s3_client = None

    def __call__(self, s3_bucket: str, s3_key: str, env: str, requirements_file: str = None,
                 requirements: list = None, config: dict = None):
        self.info('Setting up build directory')
        if not os.path.isdir('./build'):
            os.mkdir('./build')
        if os.path.isdir('./build/python-sources'):
            shutil.rmtree('./build/python-sources', ignore_errors=True)
        os.mkdir('./build/python-sources')

        self.info('Installing source files')
        # TODO use setup.py instead?
        import subprocess
        if requirements is not None:
            subprocess.call(['pip', 'install'] + requirements + ['-t', './build/python-sources'])
        else:
            subprocess.call([
                'pip', 'install',
                '-r', requirements_file or 'requirements.txt',
                '-t', './build/python-sources'
            ])

        self.info('Packaging artifact')
        subprocess.call(['cp', 'templates/aws/handlers.py', 'build/python-sources/.'])
        os.chdir('./build/python-sources')
        with open('firefly.yml', 'w') as fp:
            if config is not None:
                fp.write(yaml.dump(config))
            else:
                fp.write(yaml.dump(self._default_config(s3_bucket, env)))

        subprocess.call(['find', '.', '-name', '"*.so"', '|', 'xargs', 'strip'])
        subprocess.call(['find', '.', '-name', '"*.so.*"', '|', 'xargs', 'strip'])
        subprocess.call(['find', '.', '-name', '"*.pyc"', '-delete'])
        file_name = s3_key.split('/')[-1]
        subprocess.call(['zip', '-r', f'../{file_name}', '.'])
        os.chdir('..')

        self.info('Uploading artifact')
        with open(file_name, 'rb') as fp:
            self._s3_client.put_object(
                Body=fp.read(),
                Bucket=s3_bucket,
                Key=s3_key
            )
        subprocess.call(['rm', file_name])
        os.chdir('..')

        self._clean_up_old_artifacts(s3_bucket, '/'.join(s3_key.split('/')[:-1]))

    def _clean_up_old_artifacts(self, s3_bucket: str, code_path: str):
        response = self._s3_client.list_objects(
            Bucket=s3_bucket,
            Prefix=f'{code_path}/'
        )

        files = []
        for row in response['Contents']:
            files.append((row['Key'], row['LastModified']))
        if len(files) < 3:
            return

        files.sort(key=lambda i: i[1], reverse=True)
        for key, _ in files[2:]:
            self._s3_client.delete_object(Bucket=s3_bucket, Key=key)

    def _default_config(self, s3_bucket: str, env: str):
        return {
            'project': self._configuration.all.get('project'),
            'provider': 'aws',
            'contexts': {
                'firefly': '',
                'firefly_aws': {
                    'region': self._configuration.contexts.get('firefly_aws').get('region'),
                    'bucket': s3_bucket,
                    'is_extension': True,
                }
            },
            'environments': {
                env: '~'
            }
        }
