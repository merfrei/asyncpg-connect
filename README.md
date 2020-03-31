### Asyncpg Connect

Utilities to help with the integration of `asyncpg` in your project or scripts

#### Example

```python
from asyncpg_connect.db import DBSession

POSTGRES_URI = 'postgresql://user:password@localhost:5432/dbname'

async with DBSession(POSTGRES_URI) as session:
    results = await session.connection.fetch('SELECT * FROM mytable')

```

