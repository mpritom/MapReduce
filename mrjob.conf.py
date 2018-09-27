runners:
    emr:
        bootstrap:
        - sudo yum install -y python27-numpy
        - sudo yum install -y python27-scipy
        core_instance_type: m3.xlarge
        num_core_instances: 5
        aws_access_key_id: AKIAJ5RU3QX5JRP2QHQA
        aws_secret_access_key: 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
        cloud_tmp_dir: 