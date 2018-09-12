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


@task(pre=[profile_check])
def create(ctx, stack_name, oc_cluster, code_bucket):
    vpc_subnet_id, vpc_sg_id = vpc_components(ctx, oc_cluster)
    params = {
        'VpcSubnetId': vpc_subnet_id,
        'VpcSecurityGroupId': vpc_sg_id,
        'CloudwatchLogGroup': "{}_opencast".format(oc_cluster),
        'CodeBucket': code_bucket
    }
    __create_or_update(ctx, "create", stack_name, params)
    __wait_for(ctx, "create", stack_name)


@task(pre=[profile_check])
def update(ctx, stack_name):
    __create_or_update(ctx, "update")
    __wait_for(ctx, "update", stack_name)


@task(pre=[profile_check])
def update_lambda(ctx):
    __package(ctx)
    ctx.run("aws {} lambda update-function-code "
            "--function-name {} --s3-bucket {} --s3-key {}"
            .format(profile_arg(),
                    "stack-nag-function",
                    getenv('LAMBDA_CODE_BUCKET'),
                    "stack-nag.zip"))


@task(pre=[profile_check])
def delete(ctx, stack_name):
    cmd = "aws {} cloudformation delete-stack --stack-name {}"\
          .format(profile_arg(), stack_name)
    ctx.run(cmd)
    __wait_for(ctx, "delete", stack_name)


ns = Collection()
ns.add_task(create)
ns.add_task(update)
ns.add_task(delete)
ns.add_task(update_lambda)


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
           "'Name=tag:aws:cloudformation:logical-id,Values=PrivateSubnet*' " 
           "--query 'Subnets[0].SubnetId' --output text") \
        .format(profile_arg(), vpc_id)

    subnet_id = ctx.run(cmd, hide=1).stdout.strip()

    cmd = ("aws {} ec2 describe-security-groups --filters "
           "'Name=vpc-id,Values={}' "
           "'Name=tag:aws:cloudformation:logical-id,Values=OpsworksLayerSecurityGroupCommon' "
           "--query 'SecurityGroups[0].GroupId' --output text") \
        .format(profile_arg(), vpc_id)

    sg_id = ctx.run(cmd, hide=1).stdout.strip()

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

    __package(ctx, stack_name, params['CodeBucket'])

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
    ctx.run(cmd)


def __package(ctx, stack_name, code_bucket):

    req_file = join(dirname(__file__), 'function-requirements.txt')
    zip_path = join(dirname(__file__), 'dist/function.zip')
    build_path = join(dirname(__file__), 'dist')
    module_path = join(dirname(__file__), 'function.py')
    module_dist_path = join(build_path, 'function.py')

    if exists(build_path):
        shutil.rmtree(build_path)

    if exists(req_file):
        ctx.run("pip install --no-cache-dir -U -r {} -t {}".format(req_file, build_path))
    else:
        ctx.run("mkdir {}".format(build_path))

    try:
        print("symlinking {} to {}".format(module_path, module_dist_path))
        symlink(module_path, module_dist_path)
    except FileExistsError:
        pass

    with ctx.cd(build_path):
        ctx.run("zip -r {} .".format(zip_path))

    ctx.run("aws {} s3 cp {} s3://{}/oc-workflow-metrics/{}/function.zip" \
        .format(
            profile_arg(),
            zip_path,
            code_bucket,
            stack_name
        )
    )

def __wait_for(ctx, op, stack_name):
    wait_cmd = ("aws {} cloudformation wait stack-{}-complete "
                "--stack-name {}").format(profile_arg(), op, stack_name)
    print("Waiting for stack {} to complete...".format(op))
    ctx.run(wait_cmd)
    print("Done")

