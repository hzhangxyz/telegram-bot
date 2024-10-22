from __future__ import annotations
import sys
import time
import functools
import typing
import logging
import asyncio
import telegram
import telegram.ext
import sqlalchemy
import sqlalchemy.orm
import sqlalchemy.ext.asyncio


class Base(sqlalchemy.ext.asyncio.AsyncAttrs, sqlalchemy.orm.DeclarativeBase):
    pass


class Ids:
    id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(primary_key=True)

    @classmethod
    async def contain(cls, id: int, session: sqlalchemy.ext.asyncio.async_sessionmaker[sqlalchemy.ext.asyncio.AsyncSession]) -> bool:
        async with session() as sess:
            result = await sess.execute(sqlalchemy.select(cls).filter_by(id=id))
            return result.scalars().first() is not None

    @classmethod
    async def list(cls, session: sqlalchemy.ext.asyncio.async_sessionmaker[sqlalchemy.ext.asyncio.AsyncSession]) -> list[int]:
        async with session() as sess:
            result = await sess.execute(sqlalchemy.select(cls))
            return [ids.id for ids in result.scalars()]

    @classmethod
    async def add(cls, id: int, session: sqlalchemy.ext.asyncio.async_sessionmaker[sqlalchemy.ext.asyncio.AsyncSession]) -> None:
        async with session() as sess:
            sess.add(cls(id=id))
            await sess.commit()

    @classmethod
    async def remove(cls, id: int, session: sqlalchemy.ext.asyncio.async_sessionmaker[sqlalchemy.ext.asyncio.AsyncSession]) -> None:
        async with session() as sess:
            result = await sess.execute(sqlalchemy.select(cls).filter_by(id=id))
            obj = result.scalars().one()
            await sess.delete(obj)
            await sess.commit()


class Admin(Base, Ids):
    __tablename__ = "admin"


class User(Base, Ids):
    __tablename__ = "user"


class Message(Base):
    __tablename__ = "message"
    chat_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(primary_key=True)
    msg_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(primary_key=True)
    user_id: sqlalchemy.orm.Mapped[int]
    is_me: sqlalchemy.orm.Mapped[bool]
    text: sqlalchemy.orm.Mapped[str] = sqlalchemy.orm.mapped_column(sqlalchemy.Text)
    model: sqlalchemy.orm.Mapped[str | None] = sqlalchemy.orm.mapped_column(sqlalchemy.String(32))
    reply_id: sqlalchemy.orm.Mapped[int | None]

    async def add(self, session: sqlalchemy.ext.asyncio.async_sessionmaker[sqlalchemy.ext.asyncio.AsyncSession]) -> None:
        async with session() as sess:
            sess.add(self)
            await sess.commit()

    async def history(self, session: sqlalchemy.ext.asyncio.async_sessionmaker[sqlalchemy.ext.asyncio.AsyncSession]) -> list[Message]:
        async with session() as sess:
            result = [self]
            msg_id = self.reply_id
            while msg_id is not None:
                query = await sess.execute(sqlalchemy.select(Message).filter_by(chat_id=self.chat_id, msg_id=msg_id))
                message = query.scalars().one()
                result.append(message)
                msg_id = message.reply_id

            result.reverse()
            return result

    def __str__(self) -> str:
        return f"[chat_id={self.chat_id} msg_id={self.msg_id} user_id={self.user_id}]: {self.text} -> {self.model or self.reply_id}"

    def __repr__(self) -> str:
        return str(self)


class PendingPool:

    def __init__(self, logger: logging.Logger) -> None:
        self.logger: logging.Logger = logger
        self.pool: dict[tuple[int, int], asyncio.Event] = {}

    def add(self, key: tuple[int, int]) -> None:
        assert key not in self.pool
        self.pool[key] = asyncio.Event()

    def remove(self, key: tuple[int, int]) -> None:
        if key not in self.pool:
            return
        self.pool[key].set()
        del self.pool[key]

    async def wait_for(self, key: tuple[int, int]) -> None:
        if key not in self.pool:
            return
        self.logger.info('Waiting for %r', key)
        await self.pool[key].wait()
        self.logger.info('Waiting for %r finished', key)


Handle = typing.Callable[["BotApp", telegram.Update, telegram.ext.CallbackContext], typing.Awaitable[None]]
Model = typing.Callable[[list[Message]], typing.AsyncGenerator[str, None]]


def only_admin(func: Handle) -> Handle:

    @functools.wraps(func)
    async def wrapper(self: "BotApp", update: telegram.Update, context: telegram.ext.CallbackContext) -> None:
        if await self._is_admin(update.message.from_user.id):
            await func(self, update, context)
        else:
            await self._send_message(update.effective_chat.id, 'only admin', update.message.message_id)

    return wrapper


def only_user(func: Handle) -> Handle:

    @functools.wraps(func)
    async def wrapper(self: "BotApp", update: telegram.Update, context: telegram.ext.CallbackContext) -> None:
        if await self._is_admin(update.message.from_user.id) or await self._is_user(update.effective_chat.id):
            await func(self, update, context)
        else:
            if update.effective_chat.id == update.message.from_user.id:
                await self._send_message(update.effective_chat.id, 'only user', update.message.message_id)

    return wrapper


class BotApp:

    async def _is_admin(self, id: int) -> bool:
        self.logger.info("check if %d is admin", id)
        return await Admin.contain(id, self.database_session)

    async def _add_admin(self, id: int) -> None:
        self.logger.info("add %d as admin", id)
        return await Admin.add(id, self.database_session)

    async def _remove_admin(self, id: int) -> None:
        self.logger.info("remove %d as admin", id)
        return await Admin.remove(id, self.database_session)

    async def _ls_admin(self) -> list[int]:
        self.logger.info("list admin")
        return await Admin.list(self.database_session)

    async def _is_user(self, id: int) -> bool:
        self.logger.info("check if %d is user", id)
        return await User.contain(id, self.database_session)

    async def _add_user(self, id: int) -> None:
        self.logger.info("add %d as user", id)
        return await User.add(id, self.database_session)

    async def _remove_user(self, id: int) -> None:
        self.logger.info("remove %d as user", id)
        return await User.remove(id, self.database_session)

    async def _ls_user(self) -> list[int]:
        self.logger.info("list user")
        return await User.list(self.database_session)

    async def _send_message(self, chat_id: int, text: str, reply_to_message_id: int) -> int:
        self.logger.info('Sending message: chat_id=%r, reply_to_message_id=%r, text=%r', chat_id, reply_to_message_id, text)
        msg = await self.app.bot.send_message(
            chat_id,
            text,
            reply_to_message_id=reply_to_message_id,
        )
        self.logger.info('Message sent: chat_id=%r, reply_to_message_id=%r, message_id=%r', chat_id, reply_to_message_id, msg.message_id)
        return msg.message_id

    async def _edit_message(self, chat_id: int, text: str, message_id: int, parse_mode: str | None = None) -> None:
        self.logger.info('Editing message: chat_id=%r, message_id=%r, text=%r', chat_id, message_id, text)
        await self.app.bot.edit_message_text(
            text,
            chat_id=chat_id,
            message_id=message_id,
            parse_mode=parse_mode,
        )
        self.logger.info('Message edited: chat_id=%r, message_id=%r', chat_id, message_id)

    async def _ping_handle(self, update: telegram.Update, context: telegram.ext.CallbackContext) -> None:
        reply = f'chat_id={update.effective_chat.id} user_id={update.message.from_user.id}'
        await self._send_message(update.effective_chat.id, reply, update.message.message_id)

    async def _list_model_handle(self, update: telegram.Update, context: telegram.ext.CallbackContext) -> None:
        reply = "\n".join([f"prefix {p} for model {m}" for p, m in self.prefixes.items()])
        await self._send_message(update.effective_chat.id, reply, update.message.message_id)

    @only_admin
    async def _list_admin_handle(self, update: telegram.Update, context: telegram.ext.CallbackContext) -> None:
        ids = await self._ls_admin()
        if ids:
            reply = ", ".join([str(id) for id in ids])
        else:
            reply = "empty"
        await self._send_message(update.effective_chat.id, reply, update.message.message_id)

    @only_admin
    async def _add_admin_handle(self, update: telegram.Update, context: telegram.ext.CallbackContext) -> None:
        args = " ".join(context.args)
        try:
            id = int(args)
        except ValueError:
            await self._send_message(update.effective_chat.id, "value error", update.message.message_id)
            return
        if await self._is_admin(id):
            await self._send_message(update.effective_chat.id, "is admin already", update.message.message_id)
            return
        await self._add_admin(id)
        await self._send_message(update.effective_chat.id, "done", update.message.message_id)

    @only_admin
    async def _remove_admin_handle(self, update: telegram.Update, context: telegram.ext.CallbackContext) -> None:
        args = " ".join(context.args)
        try:
            id = int(args)
        except ValueError:
            await self._send_message(update.effective_chat.id, "value error", update.message.message_id)
            return
        if not await self._is_admin(id):
            await self._send_message(update.effective_chat.id, "is not admin yet", update.message.message_id)
            return
        await self._remove_admin(id)
        await self._send_message(update.effective_chat.id, "done", update.message.message_id)

    @only_admin
    async def _list_user_handle(self, update: telegram.Update, context: telegram.ext.CallbackContext) -> None:
        ids = await self._ls_user()
        if ids:
            reply = ", ".join([str(id) for id in ids])
        else:
            reply = "empty"
        await self._send_message(update.effective_chat.id, reply, update.message.message_id)

    @only_admin
    async def _add_user_handle(self, update: telegram.Update, context: telegram.ext.CallbackContext) -> None:
        args = " ".join(context.args)
        try:
            id = int(args)
        except ValueError:
            await self._send_message(update.effective_chat.id, "value error", update.message.message_id)
            return
        if await self._is_user(id):
            await self._send_message(update.effective_chat.id, "is user already", update.message.message_id)
            return
        await self._add_user(id)
        await self._send_message(update.effective_chat.id, "done", update.message.message_id)

    @only_admin
    async def _remove_user_handle(self, update: telegram.Update, context: telegram.ext.CallbackContext) -> None:
        args = " ".join(context.args)
        try:
            id = int(args)
        except ValueError:
            await self._send_message(update.effective_chat.id, "value error", update.message.message_id)
            return
        if not await self._is_user(id):
            await self._send_message(update.effective_chat.id, "is not user yet", update.message.message_id)
            return
        await self._remove_user(id)
        await self._send_message(update.effective_chat.id, "done", update.message.message_id)

    @only_admin
    async def _add_current_handle(self, update: telegram.Update, context: telegram.ext.CallbackContext) -> None:
        id = update.effective_chat.id
        if await self._is_user(id):
            await self._send_message(update.effective_chat.id, "is user already", update.message.message_id)
            return
        await self._add_user(id)
        await self._send_message(update.effective_chat.id, "done", update.message.message_id)

    @only_admin
    async def _remove_current_handle(self, update: telegram.Update, context: telegram.ext.CallbackContext) -> None:
        id = update.effective_chat.id
        if not await self._is_user(id):
            await self._send_message(update.effective_chat.id, "is not user yet", update.message.message_id)
            return
        await self._remove_user(id)
        await self._send_message(update.effective_chat.id, "done", update.message.message_id)

    def __init__(self, telegram_token: str, database_url: str, owner_id: int, logger: logging.Logger) -> None:
        self.telegram_token: str = telegram_token
        self.database_url: str = database_url
        self.owner_id: int = owner_id
        self.logger: logging.Logger = logger

        self.bot_id: int = int(self.telegram_token.split(':')[0])
        self.pending_pool: PendingPool = PendingPool(self.logger)
        self.prefixes: dict[str, str] = {}
        self.models: dict[str, Model] = {}

    async def _set_commands(self):
        await self.app.bot.set_my_commands([
            ('ping', 'Test bot connectivity'),
            ('list_model', 'List models'),
            ('list_admin', 'List admin (only admin)'),
            ('add_admin', 'Add admin (only admin)'),
            ('remove_admin', 'Remove admin (only admin)'),
            ('list_user', 'List user (only admin)'),
            ('add_user', 'Add user (only admin)'),
            ('remove_user', 'Remove user (only admin)'),
            ('add_current', 'Add current as user (only admin)'),
            ('remove_current', 'Remove current as user (only admin)'),
        ])

    async def run(self):
        self.logger.info("Connect to database")
        self.database_engine = sqlalchemy.ext.asyncio.create_async_engine(self.database_url)
        self.database_session = sqlalchemy.ext.asyncio.async_sessionmaker(self.database_engine, expire_on_commit=False)
        async with self.database_engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        if not await self._is_admin(self.owner_id):
            await self._add_admin(self.owner_id)

        self.logger.info("Create telegram app")
        self.app = telegram.ext.ApplicationBuilder().token(self.telegram_token).concurrent_updates(True).build()
        self.logger.info("Add command handlers")
        self.app.add_handler(telegram.ext.CommandHandler("ping", self._ping_handle))
        self.app.add_handler(telegram.ext.CommandHandler("list_model", self._list_model_handle))
        self.app.add_handler(telegram.ext.CommandHandler("list_admin", self._list_admin_handle))
        self.app.add_handler(telegram.ext.CommandHandler("add_admin", self._add_admin_handle))
        self.app.add_handler(telegram.ext.CommandHandler("remove_admin", self._remove_admin_handle))
        self.app.add_handler(telegram.ext.CommandHandler("list_user", self._list_user_handle))
        self.app.add_handler(telegram.ext.CommandHandler("add_user", self._add_user_handle))
        self.app.add_handler(telegram.ext.CommandHandler("remove_user", self._remove_user_handle))
        self.app.add_handler(telegram.ext.CommandHandler("add_current", self._add_current_handle))
        self.app.add_handler(telegram.ext.CommandHandler("remove_current", self._remove_current_handle))
        self.app.add_handler(telegram.ext.MessageHandler(telegram.ext.filters.TEXT & (~telegram.ext.filters.COMMAND), self._message_handle))

        self.logger.info("Setup the application")
        await self.app.initialize()
        await self._set_commands()
        await self.app.start()
        await self.app.updater.start_polling()
        self.logger.info("The application is running")
        while True:
            await asyncio.sleep(3600)
        self.logger.info("Stop the application")
        await self.app.updater.stop()
        await self.app.stop()
        await self.app.shutdown()

        self.logger.info("Disconnect from database")
        await engine.dispose()

    @only_user
    async def _message_handle(self, update: telegram.Update, context: telegram.ext.CallbackContext) -> None:
        chat_id = update.effective_chat.id
        msg_id = update.message.message_id
        user_id = update.message.from_user.id
        text = update.message.text
        self.logger.info('New message: chat_id=%r, user_id=%r, msg_id=%r, text=%r', chat_id, user_id, msg_id, text)

        reply_to_message = update.message.reply_to_message
        if reply_to_message is None:
            reply_id = None

            prefix = ""
            model = ""
            for p, m in self.prefixes.items():
                if text.startswith(p):
                    if prefix is None or len(prefix) < len(p):
                        prefix = p
                        model = m
            if model == "":
                if update.effective_chat.id == update.message.from_user.id:
                    await self._send_message(update.effective_chat.id, "unknown model", update.message.message_id)
                return
            text = text[len(prefix):]
            message = Message(chat_id=chat_id, msg_id=msg_id, user_id=user_id, is_me=False, text=text, model=model, reply_id=reply_id)
        elif update.message.reply_to_message.from_user.id == self.bot_id:
            reply_id = reply_to_message.message_id
            await self.pending_pool.wait_for((chat_id, reply_id))
            model = None
            message = Message(chat_id=chat_id, msg_id=msg_id, user_id=user_id, is_me=False, text=text, model=model, reply_id=reply_id)
        else:
            return

        await message.add(self.database_session)
        history = await message.history(self.database_session)
        model = typing.cast(str, history[0].model)

        async with MessageSender(self, chat_id, msg_id, model) as sender:
            reply = ""
            async for delta in self.models[model](history):
                reply += delta
                await sender.update(reply)
        new_msg_id = sender.new_msg_id

        new_message = Message(chat_id=chat_id, msg_id=new_msg_id, user_id=self.bot_id, is_me=True, text=reply, model=None, reply_id=msg_id)
        await new_message.add(self.database_session)

        await self._edit_message(chat_id, f"`[{model}]` " + reply, new_msg_id, parse_mode="Markdown")


class MessageSender:

    def __init__(self, owner: BotApp, chat_id: int, msg_id: int, model: str):
        self.owner = owner
        self.chat_id = chat_id
        self.msg_id = msg_id
        self.model = model
        self.last_update = True
        self.start = False
        self.first = True
        self.reply = ""

    async def __aenter__(self):
        return self

    async def _update(self):
        if self.first:
            self.first = False
            self.new_msg_id = await self.owner._send_message(self.chat_id, f"[{self.model}] " + self.reply, self.msg_id)
            self.owner.pending_pool.add((self.chat_id, self.new_msg_id))
        else:
            await self.owner._edit_message(self.chat_id, f"[{self.model}] " + self.reply, self.new_msg_id)

    async def update(self, reply: str):
        if self.reply == reply:
            return
        self.reply = reply
        if not self.start:
            self.target_time = time.time() + 1
            self.start = True
        current_time = time.time()
        if current_time > self.target_time:
            await self._update()
            self.last_update = False
            self.target_time = current_time + 3
        else:
            self.last_update = True

    async def __aexit__(self, *args):
        if self.last_update:
            await self._update()
        self.owner.pending_pool.remove((self.chat_id, self.new_msg_id))


def echo() -> Model:

    async def reply(history: list[Message]) -> typing.AsyncGenerator[str, None]:
        yield "echo to "
        yield history[-1].text

    return reply


def azure_gpt(*, model: str, api_key: str, endpoint: str, owner: str) -> Model:
    import openai
    import datetime

    aclient = openai.AsyncAzureOpenAI(
        api_key=api_key,
        api_version="2024-02-15-preview",
        azure_endpoint=endpoint,
        max_retries=0,
        timeout=15,
    )

    async def reply(history: list[Message]) -> typing.AsyncGenerator[str, None]:
        time = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S')
        messages = [{"role": "system", "content": f'You are {model} Telegram bot. {model} is a large language model trained by {owner}. Answer as concisely as possible. Current Beijing Time: {time}'}]
        for message in history:
            messages.append({"role": "assistant" if message.is_me else "user", "content": message.text})
        stream = await aclient.chat.completions.create(model=model, messages=messages, stream=True)
        async for response in stream:
            if response.choices:
                assert len(response.choices) == 1
                obj = response.choices[0]
                if obj.delta is not None:
                    if obj.delta.role is not None:
                        if obj.delta.role != 'assistant':
                            raise ValueError("Role error")
                    if obj.delta.content is not None:
                        yield obj.delta.content
                if obj.finish_reason is not None or ('finish_details' in obj.model_extra and obj.finish_details is not None):
                    assert all(item is None or item == "" for item in [
                        obj.delta.content,
                        obj.delta.function_call,
                        obj.delta.role,
                        obj.delta.tool_calls,
                    ])
                    finish_reason = obj.finish_reason
                    if 'finish_details' in obj.model_extra and obj.finish_details is not None:
                        assert finish_reason is None
                        finish_reason = obj.finish_details['type']
                    if finish_reason == 'length':
                        yield '\n\n[!] Error: Output truncated due to limit'
                    elif finish_reason == 'stop':
                        pass
                    elif finish_reason is not None:
                        if obj.finish_reason is not None:
                            yield f'\n\n[!] Error: finish_reason="{finish_reason}"'
                        else:
                            yield f'\n\n[!] Error: finish_details="{obj.finish_details}"'
                    return

    return reply


def gpt(*, model: str, api_key: str, endpoint: str, owner: str) -> Model:
    import openai
    import datetime

    aclient = openai.AsyncOpenAI(
        api_key=api_key,
        base_url=endpoint,
        max_retries=0,
        timeout=15,
    )

    async def reply(history: list[Message]) -> typing.AsyncGenerator[str, None]:
        time = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S')
        messages = [{"role": "system", "content": f'You are {model} Telegram bot. {model} is a large language model trained by {owner}. Answer as concisely as possible. Current Beijing Time: {time}'}]
        for message in history:
            messages.append({"role": "assistant" if message.is_me else "user", "content": message.text})
        stream = await aclient.chat.completions.create(model=model, messages=messages, stream=True)
        async for response in stream:
            if response.choices:
                assert len(response.choices) == 1
                obj = response.choices[0]
                if obj.delta is not None:
                    if obj.delta.role is not None:
                        if obj.delta.role != 'assistant':
                            raise ValueError("Role error")
                    if obj.delta.content is not None:
                        yield obj.delta.content
                if obj.finish_reason is not None or ('finish_details' in obj.model_extra and obj.finish_details is not None):
                    assert all(item is None or item == "" for item in [
                        obj.delta.content,
                        obj.delta.function_call,
                        obj.delta.role,
                        obj.delta.tool_calls,
                    ])
                    finish_reason = obj.finish_reason
                    if 'finish_details' in obj.model_extra and obj.finish_details is not None:
                        assert finish_reason is None
                        finish_reason = obj.finish_details['type']
                    if finish_reason == 'length':
                        yield '\n\n[!] Error: Output truncated due to limit'
                    elif finish_reason == 'stop':
                        pass
                    elif finish_reason is not None:
                        if obj.finish_reason is not None:
                            yield f'\n\n[!] Error: finish_reason="{finish_reason}"'
                        else:
                            yield f'\n\n[!] Error: finish_details="{obj.finish_details}"'
                    return

    return reply


if __name__ == '__main__':
    import yaml

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s :: %(levelname)s :: %(message)s')
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.INFO)
    stdout_handler.setFormatter(formatter)
    logger.addHandler(stdout_handler)
    file_handler = logging.FileHandler(f'{__file__}.log')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    with open("config.yml", "rt") as file:
        config = yaml.safe_load(file)

    app = BotApp(
        telegram_token=config["telegram"]["token"],
        database_url=config["database"]["url"],
        owner_id=config["telegram"]["owner_id"],
        logger=logger,
    )

    for model in config["models"]:
        app.prefixes[model["prefix"]] = model["name"]
        app.models[model["name"]] = globals()[model["type"]](**model["args"])

    asyncio.run(app.run())
