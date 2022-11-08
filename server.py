from __future__ import annotations

import json
from typing import Optional

import asyncpg
from aiohttp import web
import asyncio
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, func
import pydantic
from sqlalchemy.exc import IntegrityError

PG_DSN = 'postgresql+asyncpg://aiohttp:1234@127.0.0.1:5431/netologyhw'
engine = create_async_engine(PG_DSN)
Session = sessionmaker(class_=AsyncSession, bind=engine, expire_on_commit=False)


class ApiError(web.HTTPException):

    def __init__(self, error_message: str | dict):
        message = json.dumps({'status': 'error', 'description': error_message})
        super(ApiError, self).__init__(text=message, content_type='application/json')


class NotFound(ApiError):
    status_code = 404


class BadRequest(ApiError):
    status_code = 400


Base = declarative_base()

app = web.Application()


class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True)
    username = Column(String(60), unique=True, nullable=False)
    password = Column(String, nullable=False)
    advertisements = relationship('Advertisement', back_populates='user')


class Advertisement(Base):
    __tablename__ = 'advertisements'

    id = Column(Integer, primary_key=True)
    title = Column(String, nullable=False)
    description = Column(String, nullable=False)
    creation_time = Column(DateTime, server_default=func.now())
    user_id = Column(Integer, ForeignKey('users.id'))
    user = relationship(User)


async def get_user(user_id: int, session: Session):
    user = await session.get(User, user_id)
    if user is None:
        raise NotFound(error_message='user not found')
    return user


async def get_adv(adv_id: int, session: Session):
    adv = await session.get(Advertisement, adv_id)
    if adv is None:
        raise NotFound(error_message='advertisement not found')
    return adv


class CreateUserModel(pydantic.BaseModel):
    username: str
    password: str


class CreateAdvModel(pydantic.BaseModel):
    title: str
    description: str
    user_id: int


class PatchUserModel(pydantic.BaseModel):
    username: Optional[str]
    password: Optional[str]


class PatchAdvModel(pydantic.BaseModel):
    title: Optional[str]
    description: Optional[str]


class UserView(web.View):

    async def get(self):
        user_id = int(self.request.match_info['user_id'])
        async with Session() as session:
            user = await get_user(user_id, session)
            return web.json_response({'id_user': user.id, 'username': user.username})

    async def post(self):
        json_data = await self.request.json()
        json_data = CreateUserModel(**json_data).dict()
        async with Session() as session:
            new_user = User(**json_data)
            try:
                session.add(new_user)
                await session.commit()
            except IntegrityError:
                raise BadRequest(error_message='user already exists')
            return {'id': new_user.id}

    async def delete(self):
        user_id = int(self.request.match_info['user_id'])
        async with Session() as session:
            user = await get_user(user_id, session)
            await session.delete(user)
            await session.commit()
            return web.json_response({'status': 'succes'})

    async def patch(self):
        user_id = int(self.request.match_info['user_id'])
        json_data = await self.request.json()
        json_data = PatchUserModel(**json_data).dict(exclude_none=True)
        async with Session() as session:
            user = await get_user(user_id, session)

            for column, value in json_data.items():
                setattr(user, column, value)
            session.add(user)
            await session.commit()
            return web.json_response({'status': 'success'})


class AdvView(web.View):
    async def get(self):
        adv_id = int(self.request.match_info['adv_id'])
        async with Session() as session:
            adv = await get_adv(adv_id, session)
            return web.json_response({'title': adv.title, 'description': adv.description,
                                      'date': adv.creation_time.isoformat(), 'user_id': adv.user_id})

    async def post(self):
        json_data = await self.request.json()
        json_data = CreateAdvModel(**json_data).dict()
        async with Session() as session:
            new_adv = Advertisement(**json_data)
            try:
                session.add(new_adv)
                await session.commit()
            except IntegrityError:
                raise BadRequest(error_message='advertisement already exists')
            return {'id': new_adv.id}

    async def delete(self):
        adv_id = int(self.request.match_info['adv_id'])
        async with Session() as session:
            adv = await get_adv(adv_id, session)
            await session.delete(adv)
            await session.commit()
            return web.json_response({'status': 'succes'})

    async def patch(self):
        adv_id = int(self.request.match_info['adv_id'])
        json_data = await self.request.json()
        json_data = PatchAdvModel(**json_data).dict(exclude_none=True)
        async with Session() as session:
            adv = await get_adv(adv_id, session)

            for column, value in json_data.items():
                setattr(adv, column, value)
            session.add(adv)
            await session.commit()
            return web.json_response({'status': 'success'})


async def init_orm(app):
    print('START')
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.commit()
    yield
    await engine.dispose()
    print('SHUT DOWN!')


app.router.add_route('POST', '/advs/', AdvView)
app.router.add_route('GET', '/advs/{adv_id:\d+}', AdvView)
app.router.add_route('DELETE', '/advs/{adv_id:\d+}', AdvView)
app.router.add_route('PATCH', '/advs/{adv_id:\d+}', AdvView)
app.router.add_route('POST', '/users/', UserView)
app.router.add_route('GET', '/users/{user_id:\d+}', UserView)
app.router.add_route('DELETE', '/users/{user_id:\d+}', UserView)
app.router.add_route('PATCH', '/users/{user_id:\d+}', UserView)
app.cleanup_ctx.append(init_orm)
web.run_app(app)
