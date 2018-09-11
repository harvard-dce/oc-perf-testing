import shutil
from invoke import task, Collection
from invoke.exceptions import Exit
from os import symlink, getenv as env
from os.path import join, dirname, exists
import json

AWS_PROFILE = env('AWS_PROFILE')

@task
def profile_check(ctx):
    if AWS_PROFILE is None:
        print("You do not have 'AWS_PROFILE' set in your environment. "
              "This task will run using your default AWS account/credentials. "
              )
        ok = input('Is this what you want? [y/N] ').lower().strip().startswith('y')
        if not ok:
            raise Exit("Aborting")


@task
def create(ctx, stack_name, oc_cluster, code_bucket):
    vpc_subnet_id, vpc_sg_id = vpc_components(ctx, oc_cluster)
    params = {
        'VpcSubnetId': vpc_subnet_id,
        'VpcSecurityGroupId': vpc_sg_id,
        'CloudwatchLogGroup': "{}_opencast",
        'CodeBucket': code_bucket
    }
    __create_or_update(ctx, "create", stack_name, params)


@task
def update(ctx, stack_name):
    __create_or_update(ctx, "update")


@task
def update_lambda(ctx):
    __package(ctx)
    ctx.run("aws {} lambda update-function-code "
            "--function-name {} --s3-bucket {} --s3-key {}"
            .format(profile_arg(),
                    "stack-nag-function",
                    getenv('LAMBDA_CODE_BUCKET'),
                    "stack-nag.zip"))


@task
def delete(ctx, stack_name):
    cmd = "aws {} cloudformation delete-stack --stack-name {}"\
          .format(profile_arg(), stack_name)
    ctx.run(cmd)


def stack_exists(ctx, stack_name):
    cmd = "aws {} cloudformation describe-stacks --stack-name {}"\
          .format(profile_arg(), stack_name)
    res = ctx.run(cmd, hide=True, warn=True, echo=False)
    return res.exited == 0


def profile_arg():
    if AWS_PROFILE is not None:
        return "--profile {}".format(AWS_PROFILE)
    return ""


def vpc_components(ctx, oc_cluster):

    cmd = ("aws opsworks describe-stacks "
           "--query \"Stacks[?Name=='{}'].VpcId\" "
           "--output text").format(oc_cluster)
    vpc_id = ctx.run(cmd, hide=1).stdout.strip()

    if vpc_id == "":
        raise Exit("Can't find a cluster named '{}'".format(oc_cluster))

    cmd = ("aws {} ec2 describe-subnets --filters "
           "'Name=vpc-id,Values={}' "
           "'Name=tag:aws:cloudformation:logical-id,Values=PrivateSubnet'") \
        .format(profile_arg(), vpc_id)

    res = ctx.run(cmd, hide=1)
    subnet_data = json.loads(res.stdout)
    subnet_id = subnet_data['Subnets'][0]['SubnetId']

    cmd = ("aws {} ec2 describe-security-groups --filters "
           "'Name=vpc-id,Values={}' "
           "'Name=tag:aws:cloudformation:logical-id,Values=OpsworksLayerSecurityGroupCommon'") \
        .format(profile_arg(), vpc_id)
    res = ctx.run(cmd, hide=1)
    sg_data = json.loads(res.stdout)
    sg_id = sg_data['SecurityGroups'][0]['GroupId']

    return subnet_id, sg_id

def cfn_cmd_params(params):
    return " ".join(
        "ParameterKey={},ParameterValue={}".format(k, v)
        for k, v in params.items()
    )

def __create_or_update(ctx, op, stack_name, params):

    if op == "create" and stack_exists(ctx, stack_name):
        raise Exit("Stack {} already exists!".format(stack_name))

    cmd_params = cfn_cmd_params(params)

    __package(ctx)

    cmd = ("aws {} cloudformation {}-stack "
           "--capabilities CAPABILITY_NAMED_IAM "
           "--stack-name {} "
           "--template-body file://template.yml "
           "--parameters {}"
        ).format(
            profile_arg(),
            op,
            stack_name,
            cmd_params
        )
    print(cmd)
#    ctx.run(cmd)


def __package(ctx, stack_name, code_bucket):

    req_file = join(dirname(__file__), 'function-requirements.txt')
    zip_path = join(dirname(__file__), 'dist/function.zip')
    build_path = join(dirname(__file__), 'dist')
    module_path = join(dirname(__file__), 'function.py')
    module_dist_path = join(build_path, 'function.py')

    if exists(build_path):
        shutil.rmtree(build_path)

    if exists(req_file):
        ctx.run("pip install -U -r {} -t {}".format(req_file, build_path))
    else:
        ctx.run("mkdir {}".format(build_path))

    try:
        print("symlinking {} to {}".format(module_path, module_dist_path))
        symlink(module_path, module_dist_path)
    except FileExistsError:
        pass

    with ctx.cd(build_path):
        ctx.run("zip -r {} .".format(zip_path))

    ctx.run("aws {profile} s3 cp {zip_path} s3://{bucket}/oc-workflow-metrics/{stack}/function.zip" \
        .format(
            profile_arg(),
            zip_path,
            code_bucket,
            stack_name
        )
    )
