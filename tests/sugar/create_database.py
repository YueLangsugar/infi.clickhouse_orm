import click
from datetime import datetime
from src.infi.clickhouse_orm import (
    Database, Model, StringField, Int8Field, DateTimeField,
    MergeTree
)


db = Database("test")


class Users(Model):
    oid = StringField()
    name = StringField()
    age = Int8Field()
    created_time = DateTimeField()

    _database = db
    engine = MergeTree(order_by=(oid, ), date_col="created_time")


class Address(Model):
    user_oid = StringField()
    address = StringField()
    created_time = DateTimeField()

    _database = db

    engine = MergeTree(order_by=(user_oid, ), date_col="created_time")


class UploadSchema(Model):
    datasource_id = StringField()
    created_time = DateTimeField()

    _database = db
    engine = MergeTree(order_by=(datasource_id, ), partition_key=["xxHash64(datasource_id) % 256"])


@click.group()
def cli():
    pass


@cli.command()
def create_upload_schema():
    click.echo("upload_schema创建开始")
    db.create_table(UploadSchema)
    click.echo("upload_schema创建完毕")


@cli.command("create_database")
def create_database():
    click.echo("create_database")
    db.create_database()
    db.create_table(Users)
    click.echo("create_database end")


@cli.command()
def create_address_table():
    click.echo("开始创建表address")
    db.create_table(Address)
    click.echo("结束创建表address")


@cli.command()
def insert_address():
    click.echo("开始插入地址信息")
    data = [
        {
            "user_oid": "5b6020bdc7f17b323e33b239",
            "address": "家住太湖边, 有田又有钱",
            "created_time": datetime.now()
        },
        {
            "user_oid": "5b6020bdc7f17b323e33b239",
            "address": "安徽凤阳小岗村",
            "created_time": datetime.now()
        },
        {
            "user_oid": "5b6020bdc7f17b323e33b23c",
            "address": "路尽隐香处, 翩然雪海间",
            "created_time": datetime.now()
        }
    ]
    db.insert([Address(**d) for d in data])
    click.echo("地址信息插入完毕")


@cli.command()
def insert_test():
    click.echo("开始插入")
    data = [
        {
            "oid": "5b6020bdc7f17b323e33b239",
            "name": "sugar",
            "age": 18,
            "created_time": datetime.now()
        },
        {
            "oid": "5b6020bdc7f17b323e33b23c",
            "name": "python",
            "age": 26,
            "created_time": datetime.now()
        },
        {
            "oid": "5b6020bdc7f17b323e33b23d",
            "name": "java",
            "age": 25,
            "created_time": datetime.now()
        }
    ]

    db.insert([Users(**d) for d in data])
    click.echo("插入完毕")


@cli.command()
def query_test():
    click.echo("开始进行单表查询")
    queryset = Users.objects_in(db)
    res = queryset.filter(Users.oid == "5b6020bdc7f17b323e33b239")
    print("res=", res, type(res))
    for r in res:
        print(r.to_dict())
    click.echo("结束单表查询")

# import peewee
# class Test(peewee.Model):
#     name =
#     pass
#
# Test.select().join()

@cli.command()
def query_join_test():
    click.echo("开始进行join链表查询")
    queryset = Users.objects_in(db)
    res = queryset.join(Address, strictness="ALL", join_type="INNER", on=(
        Users.oid == Address.user_oid
    ))
    for r in res:
        print("r==", r.to_dict())
    click.echo("join链表查询结束")
    pass


@cli.command("model_fields")
def query_model_fields():
    click.echo("获取模型的字段")
    res = Users.fields()
    for r in res:
        print("res", r, type(r))
    click.echo("获取模型字段结束")


# todo: 多表字段相同问题处理、select * 问题字段对应关系处理
@cli.command()
def select_query():
    res = Users.select(
        Users.name,
        Users.oid,
        Users.age,
        Users.created_time,
        Address.address,
        Address.created_time
    ).join(
        Address, on=(Users.oid == Address.user_oid)
    )
    for r in res:
        print('r', r)
        # print(r.to_dict())
        pass


if __name__ == '__main__':
    # cli()
    # query_model_fields()
    select_query()
