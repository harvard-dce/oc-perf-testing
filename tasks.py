import shutil
from invoke import task, Collection
from invoke.exceptions import Exit
from os import symlink, getenv as env
from os.path import join, dirname, exists
from dotenv import load_dotenv
import json

load_dotenv(join(dirname(__file__), '.env'))

AWS_PROFILE = env('AWS_PROFILE')
AWS_DEFAULT_REGION = env('AWS_DEFAULT_REGION', 'us-east-1')


def getenv(var, required=True):
    val = env(var)
    if required and val is None:
        raise Exit("{} not defined".format(var))
    return val


def profile_arg():
    if AWS_PROFILE is not None:
        return "--profile {}".format(AWS_PROFILE)
    return ""


def stack_tags():
    tags = "Key=cfn-stack,Value={}".format(getenv('STACK_NAME'))
    extra_tags = getenv("STACK_TAGS")
    if extra_tags is not None:
        tags += " " + extra_tags
    return "--tags {}".format(tags)


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
def create(ctx):
    vpc_subnet_id, vpc_sg_id = vpc_components(ctx)
    oc_admin_ip = get_admin_ip(ctx)
    params = {
        'VpcSubnetId': vpc_subnet_id,
        'VpcSecurityGroupId': vpc_sg_id,
        'CloudwatchLogGroup': "{}_opencast".format(getenv('OC_CLUSTER')),
        'CodeBucket': getenv('CODE_BUCKET'),
        'OCAdminIp': oc_admin_ip,
        'OCCluster': getenv('OC_CLUSTER')
    }
    __create_or_update(ctx, "create", params)
    __wait_for(ctx, "create")


#@task(pre=[profile_check])
#def update(ctx, stack_name):
#    __create_or_update(ctx, "update")
#    __wait_for(ctx, "update", stack_name)


@task(pre=[profile_check])
def update_lambda(ctx):
    __package(ctx)
    function_name = getenv('STACK_NAME') + "-function"
    s3_key = "oc-workflow-metrics/" + getenv('STACK_NAME') + "/function.zip"
    cmd = ("aws {} lambda update-function-code "
            "--function-name {} --s3-bucket {} --s3-key {}") \
            .format(profile_arg(), function_name, getenv('CODE_BUCKET'), s3_key)
    ctx.run(cmd)

@task(pre=[profile_check])
def delete(ctx):
    cmd = "aws {} cloudformation delete-stack --stack-name {}"\
          .format(profile_arg(), getenv('STACK_NAME'))
    ctx.run(cmd)
    __wait_for(ctx, "delete", getenv('STACK_NAME'))


ns = Collection()
ns.add_task(create)
ns.add_task(delete)
ns.add_task(update_lambda)
#ns.add_task(update)


def stack_exists(ctx):
    cmd = "aws {} cloudformation describe-stacks --stack-name {}"\
          .format(profile_arg(), getenv('STACK_NAME'))
    res = ctx.run(cmd, hide=True, warn=True, echo=False)
    return res.exited == 0


def vpc_components(ctx):

    cmd = ("aws opsworks describe-stacks "
           "--query \"Stacks[?Name=='{}'].VpcId\" "
           "--output text").format(getenv('OC_CLUSTER'))
    vpc_id = ctx.run(cmd, hide=True).stdout.strip()

    if vpc_id == "":
        raise Exit("Can't find a cluster named '{}'".format(getenv('OC_CLUSTER')))

    cmd = ("aws {} ec2 describe-subnets --filters "
           "'Name=vpc-id,Values={}' "
           "'Name=tag:aws:cloudformation:logical-id,Values=PrivateSubnet*' " 
           "--query 'Subnets[0].SubnetId' --output text") \
        .format(profile_arg(), vpc_id)

    subnet_id = ctx.run(cmd, hide=True).stdout.strip()

    cmd = ("aws {} ec2 describe-security-groups --filters "
           "'Name=vpc-id,Values={}' "
           "'Name=tag:aws:cloudformation:logical-id,Values=OpsworksLayerSecurityGroupCommon' "
           "--query 'SecurityGroups[0].GroupId' --output text") \
        .format(profile_arg(), vpc_id)

    sg_id = ctx.run(cmd, hide=True).stdout.strip()

    return subnet_id, sg_id


def get_admin_ip(ctx):

    cmd = ("aws {} opsworks describe-stacks "
           "--query \"Stacks[?Name=='{}'].StackId\" "
           "--output text").format(profile_arg(), getenv('OC_CLUSTER'))

    opsworks_stack_id = ctx.run(cmd, hide=True).stdout.strip()

    cmd = ("aws {} opsworks describe-instances --stack-id {} "
           "--query \"Instances[?starts_with(Hostname, 'admin')].ElasticIp\" "
           "--output text").format(profile_arg(), opsworks_stack_id)

    return ctx.run(cmd, hide=True).stdout.strip()


def cfn_cmd_params(params):
    return " ".join(
        "ParameterKey={},ParameterValue={}".format(k, v)
        for k, v in params.items()
    )


def __create_or_update(ctx, op, params):

    if op == "create" and stack_exists(ctx):
        raise Exit("Stack {} already exists!".format(getenv('STACK_NAME')))

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
            getenv('STACK_NAME'),
            cmd_params
        )
    ctx.run(cmd)


def __package(ctx):

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
            getenv('CODE_BUCKET'),
            getenv('STACK_NAME')
        )
    )


def __wait_for(ctx, op):
    wait_cmd = ("aws {} cloudformation wait stack-{}-complete "
                "--stack-name {}").format(profile_arg(), op, getenv('STACK_NAME'))
    print("Waiting for stack {} to complete...".format(op))
    ctx.run(wait_cmd)
    print("Done")

