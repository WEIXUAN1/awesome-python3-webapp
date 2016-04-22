import asyncio,logging
import aiomysql
def log(sql,arg):
    logging.info('SQL:%s'%sql)

@asyncio.coroutine
def create_pool(loop,**kw):
    logging.info('create databases connection pool ..')
    global _pool
    _pool = yield from asyncio.create_pool(
        host = kw.get('host','localhost'),
        port = kw.get('port',3306),
        user = kw['user'],
        password = kw['password'],
        db = kw['db'],
        charset =kw.get('charset','stf8'),
        autocommit = kw.get('autocommit',True),
        maxsize = kw.get('maxsize',10),
        minisize = kw.get('minisize',1),
        loop =loop
    )

@asyncio.coroutine
async  def select (sql,args,size = None):
    log(sql,args)
    global _pool
    async with _pool.get() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql.replace('?','%s'),args or ())
            if size:
                rs = await  cur.fetchmany(size)
            else:
                rs = await cur.fetchchall()
        logging.info('rows returned:%s' % len(rs))
        return rs
async def execute(sql,args,autocommit =True):
    log(sql)
    async with _pool.get() as conn:
        if not autocommit:
            await conn.begin()
        try:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await  cur.execute(sql.replace('?','%s'),args)
                affected = cur.rowcount
            if not autocommit:
                await conn.commit()
        except BaseException as e:
            if not autocommit:
                await conn.rollback()
            raise
        return affected

def create_args_string(num):
    L = []
    for n in range(num):
        L.append('?')
    return ','.join(L)



