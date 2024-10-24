from __future__ import annotations
import sys
import time
import json
import datetime
import functools
import typing
import logging
import asyncio
import httpx
import telegram
import telegram.ext
import sqlalchemy
import sqlalchemy.orm
import sqlalchemy.ext.asyncio


class Base(sqlalchemy.ext.asyncio.AsyncAttrs, sqlalchemy.orm.DeclarativeBase):
    pass


class Ids:
    id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(sqlalchemy.BigInteger, primary_key=True)

    @classmethod
    async def contain(cls, id: int, session: sqlalchemy.ext.asyncio.async_sessionmaker[sqlalchemy.ext.asyncio.AsyncSession]) -> bool:
        async with session() as sess:
            result = await sess.execute(sqlalchemy.select(cls).filter_by(id=id))
            return result.scalar_one_or_none() is not None

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
            obj = result.scalar_one()
            await sess.delete(obj)
            await sess.commit()


class Admin(Base, Ids):
    __tablename__ = "admin"


class User(Base, Ids):
    __tablename__ = "user"


class Message(Base):
    __tablename__ = "message"
    chat_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(sqlalchemy.BigInteger, primary_key=True)
    msg_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(sqlalchemy.BigInteger, primary_key=True)
    user_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(sqlalchemy.BigInteger)
    is_me: sqlalchemy.orm.Mapped[bool] = sqlalchemy.orm.mapped_column(sqlalchemy.Boolean)
    text: sqlalchemy.orm.Mapped[str] = sqlalchemy.orm.mapped_column(sqlalchemy.Text)
    model: sqlalchemy.orm.Mapped[str | None] = sqlalchemy.orm.mapped_column(sqlalchemy.String(32))
    reply_id: sqlalchemy.orm.Mapped[int | None] = sqlalchemy.orm.mapped_column(sqlalchemy.BigInteger)

    async def add(self, session: sqlalchemy.ext.asyncio.async_sessionmaker[sqlalchemy.ext.asyncio.AsyncSession]) -> None:
        async with session() as sess:
            sess.add(self)
            await sess.commit()

    async def history(self, session: sqlalchemy.ext.asyncio.async_sessionmaker[sqlalchemy.ext.asyncio.AsyncSession]) -> list[Message] | None:
        async with session() as sess:
            result = [self]
            msg_id = self.reply_id
            while msg_id is not None:
                query = await sess.execute(sqlalchemy.select(self.__class__).filter_by(chat_id=self.chat_id, msg_id=msg_id))
                message = query.scalar_one_or_none()
                if message is None:
                    return None
                result.append(message)
                msg_id = message.reply_id

            result.reverse()
            return result

    def __str__(self) -> str:
        return f"[chat_id={self.chat_id} msg_id={self.msg_id} user_id={self.user_id}]: {self.text} -> {self.model or self.reply_id}"

    def __repr__(self) -> str:
        return str(self)


class Template(Base):
    __tablename__ = "template"

    default_template = "You are {model} Telegram bot. {model} is a large language model trained by {owner}. Answer as concisely as possible. Current Beijing Time: {time}"
    chat_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(sqlalchemy.BigInteger, primary_key=True)
    user_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(sqlalchemy.BigInteger, primary_key=True)
    template: sqlalchemy.orm.Mapped[str] = sqlalchemy.orm.mapped_column(sqlalchemy.Text, default=default_template)

    @classmethod
    async def set_template(cls, session: sqlalchemy.ext.asyncio.async_sessionmaker[sqlalchemy.ext.asyncio.AsyncSession], chat_id: int, user_id: int, template: str) -> None:
        async with session() as sess:
            query = await sess.execute(sqlalchemy.select(cls).filter_by(chat_id=chat_id, user_id=user_id))
            result = query.scalar_one_or_none()
            if result is None:
                obj = cls(chat_id=chat_id, user_id=user_id, template=template)
                sess.add(obj)
            else:
                result.template = template
            await sess.commit()

    @classmethod
    async def get_template(cls, session: sqlalchemy.ext.asyncio.async_sessionmaker[sqlalchemy.ext.asyncio.AsyncSession], chat_id: int, user_id: int) -> str:
        async with session() as sess:
            query = await sess.execute(sqlalchemy.select(cls).filter_by(chat_id=chat_id, user_id=user_id))
            result = query.scalar_one_or_none()
            if result is None:
                return cls.default_template
            else:
                return result.template

    @classmethod
    async def delete_template(cls, session: sqlalchemy.ext.asyncio.async_sessionmaker[sqlalchemy.ext.asyncio.AsyncSession], chat_id: int, user_id: int) -> None:
        async with session() as sess:
            query = await sess.execute(sqlalchemy.select(cls).filter_by(chat_id=chat_id, user_id=user_id))
            result = query.scalar_one_or_none()
            if result is not None:
                await sess.delete(result)
                await sess.commit()


class PendingPool:

    def __init__(self, logger: logging.Logger) -> None:
        self.logger: logging.Logger = logger
        self.pool: dict[tuple[int, int], asyncio.Event] = {}
        self.last_reply: dict[int, int] = {}

    def add(self, key: tuple[int, int]) -> None:
        assert key not in self.pool
        self.pool[key] = asyncio.Event()
        self.last_reply[key[0]] = key[1]

    def remove(self, key: tuple[int, int]) -> None:
        if key not in self.pool:
            return
        self.pool[key].set()
        del self.pool[key]

    def get_last(self, chat_id: int) -> int | None:
        return self.last_reply.get(chat_id, None)

    async def wait_for(self, key: tuple[int, int]) -> None:
        if key not in self.pool:
            return
        self.logger.info("Waiting for %r", key)
        await self.pool[key].wait()
        self.logger.info("Waiting for %r finished", key)


Handle = typing.Callable[["BotApp", telegram.Update, telegram.ext.CallbackContext], typing.Awaitable[None]]
Model = typing.Callable[[list[Message], str], typing.AsyncGenerator[str, None]]


def only_admin(func: Handle) -> Handle:

    @functools.wraps(func)
    async def wrapper(self: "BotApp", update: telegram.Update, context: telegram.ext.CallbackContext) -> None:
        if await self._is_admin(update.message.from_user.id):
            await func(self, update, context)
        else:
            await self._send_message(update.effective_chat.id, "only admin", update.message.message_id)

    return wrapper


def only_user(func: Handle) -> Handle:

    @functools.wraps(func)
    async def wrapper(self: "BotApp", update: telegram.Update, context: telegram.ext.CallbackContext) -> None:
        if await self._is_admin(update.message.from_user.id) or await self._is_user(update.effective_chat.id):
            await func(self, update, context)
        else:
            if update.effective_chat.id == update.message.from_user.id:
                await self._send_message(update.effective_chat.id, "only user", update.message.message_id)

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

    async def _send_message(self, chat_id: int, text: str, reply_to_message_id: int, parse_mode: str | None = None) -> int:
        self.logger.info("Sending message: chat_id=%r, reply_to_message_id=%r, text=%r", chat_id, reply_to_message_id, text)
        msg = await self.app.bot.send_message(
            chat_id,
            text,
            reply_to_message_id=reply_to_message_id,
            parse_mode=parse_mode,
        )
        self.logger.info("Message sent: chat_id=%r, reply_to_message_id=%r, message_id=%r", chat_id, reply_to_message_id, msg.message_id)
        return msg.message_id

    async def _edit_message(self, chat_id: int, text: str, message_id: int, parse_mode: str | None = None) -> None:
        self.logger.info("Editing message: chat_id=%r, message_id=%r, text=%r", chat_id, message_id, text)
        await self.app.bot.edit_message_text(
            text,
            chat_id=chat_id,
            message_id=message_id,
            parse_mode=parse_mode,
        )
        self.logger.info("Message edited: chat_id=%r, message_id=%r", chat_id, message_id)

    async def _ping_handle(self, update: telegram.Update, context: telegram.ext.CallbackContext) -> None:
        reply = f"chat_id={update.effective_chat.id} user_id={update.message.from_user.id}"
        await self._send_message(update.effective_chat.id, reply, update.message.message_id)

    async def _help_handle(self, update: telegram.Update, context: telegram.ext.CallbackContext) -> None:
        reply = f"This is a Telegram bot. By sending messages with a specific prefix to this bot, you can invoke a corresponding large language model and receive a reply. The specific models can be viewed by typing [/list_model]. Messages replied to the bot can enable continuous conversations, where messages prefixed with `{self.continue_prefix}` will be automatically regarded as replies to the bot's previous message. The owner of this robot is [this](tg://user?id={self.owner_id})."
        await self._send_message(update.effective_chat.id, reply, update.message.message_id, parse_mode="Markdown")

    async def _list_model_handle(self, update: telegram.Update, context: telegram.ext.CallbackContext) -> None:
        reply = "\n".join([f"`{p}` : {m}" for p, m in self.prefixes.items()])
        await self._send_message(update.effective_chat.id, reply, update.message.message_id, parse_mode="Markdown")

    async def _get_template_handle(self, update: telegram.Update, context: telegram.ext.CallbackContext) -> None:
        reply = await Template.get_template(self.database_session, update.effective_chat.id, update.message.from_user.id)
        await self._send_message(update.effective_chat.id, reply, update.message.message_id)

    async def _set_template_handle(self, update: telegram.Update, context: telegram.ext.CallbackContext) -> None:
        reply = await Template.get_template(self.database_session, update.effective_chat.id, update.message.from_user.id)
        args = " ".join(context.args)
        await Template.set_template(self.database_session, update.effective_chat.id, update.message.from_user.id, args)
        await self._send_message(update.effective_chat.id, args, update.message.message_id)

    async def _reset_template_handle(self, update: telegram.Update, context: telegram.ext.CallbackContext) -> None:
        reply = await Template.get_template(self.database_session, update.effective_chat.id, update.message.from_user.id)
        await Template.delete_template(self.database_session, update.effective_chat.id, update.message.from_user.id)
        await self._send_message(update.effective_chat.id, "reset", update.message.message_id)

    @only_admin
    async def _list_admin_handle(self, update: telegram.Update, context: telegram.ext.CallbackContext) -> None:
        ids = await self._ls_admin()
        if ids:
            reply = ", ".join([f"{id}" if id < 0 else f"[{id}](tg://user?id={id})" for id in ids])
        else:
            reply = "empty"
        await self._send_message(update.effective_chat.id, reply, update.message.message_id, parse_mode="Markdown")

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
            reply = ", ".join([f"{id}" if id < 0 else f"[{id}](tg://user?id={id})" for id in ids])
        else:
            reply = "empty"
        await self._send_message(update.effective_chat.id, reply, update.message.message_id, parse_mode="Markdown")

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

    def __init__(self, telegram_token: str, database_url: str, owner_id: int, continue_prefix: str, logger: logging.Logger, proxy: str | None = None) -> None:
        self.telegram_token: str = telegram_token
        self.database_url: str = database_url
        self.owner_id: int = owner_id
        self.continue_prefix: str = "-"
        self.logger: logging.Logger = logger
        self.proxy: str | None = proxy

        self.bot_id: int = int(self.telegram_token.split(":")[0])
        self.pending_pool: PendingPool = PendingPool(self.logger)
        self.prefixes: dict[str, str] = {}
        self.models: dict[str, Model] = {}

    async def _set_commands(self):
        await self.app.bot.set_my_commands([
            ("help", "Get help"),
            ("ping", "Test bot connectivity"),
            ("list_model", "List models"),
            ("get_template", "Get template system"),
            ("set_template", "Set template system"),
            ("reset_template", "Reset template system"),
            ("list_admin", "List admin (only admin)"),
            ("add_admin", "Add admin (only admin)"),
            ("remove_admin", "Remove admin (only admin)"),
            ("list_user", "List user (only admin)"),
            ("add_user", "Add user (only admin)"),
            ("remove_user", "Remove user (only admin)"),
            ("add_current", "Add current as user (only admin)"),
            ("remove_current", "Remove current as user (only admin)"),
        ])

    async def run(self):
        self.logger.info("Connect to database")
        database_engine = sqlalchemy.ext.asyncio.create_async_engine(self.database_url)
        self.database_session = sqlalchemy.ext.asyncio.async_sessionmaker(database_engine, expire_on_commit=False)
        async with database_engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        if not await self._is_admin(self.owner_id):
            await self._add_admin(self.owner_id)

        self.logger.info("Create telegram app")
        builder = telegram.ext.ApplicationBuilder()
        builder.token(self.telegram_token)
        builder.concurrent_updates(True)
        if self.proxy is not None:
            builder.proxy(self.proxy)
            builder.get_updates_proxy(self.proxy)
        self.app = builder.build()
        self.logger.info("Add command handlers")
        self.app.add_handler(telegram.ext.CommandHandler("ping", self._ping_handle))
        self.app.add_handler(telegram.ext.CommandHandler("help", self._help_handle))
        self.app.add_handler(telegram.ext.CommandHandler("start", self._help_handle))
        self.app.add_handler(telegram.ext.CommandHandler("get_template", self._get_template_handle))
        self.app.add_handler(telegram.ext.CommandHandler("set_template", self._set_template_handle))
        self.app.add_handler(telegram.ext.CommandHandler("reset_template", self._reset_template_handle))
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
        try:
            while True:
                await asyncio.sleep(3600)
        except asyncio.CancelledError:
            self.logger.info("Keyboard interrupt")
        self.logger.info("Stop the application")
        await self.app.updater.stop()
        await self.app.stop()
        await self.app.shutdown()

        self.logger.info("Disconnect from database")
        await database_engine.dispose()

    @only_user
    async def _message_handle(self, update: telegram.Update, context: telegram.ext.CallbackContext) -> None:
        chat_id = update.effective_chat.id
        msg_id = update.message.message_id
        user_id = update.message.from_user.id
        text = update.message.text
        self.logger.info("New message: chat_id=%r, user_id=%r, msg_id=%r, text=%r", chat_id, user_id, msg_id, text)

        reply_to_message = update.message.reply_to_message
        if reply_to_message is None:
            if text.startswith(self.continue_prefix):
                text = text[len(self.continue_prefix):]
                reply_id = self.pending_pool.get_last(chat_id)
                if reply_id is None:
                    return
                await self.pending_pool.wait_for((chat_id, reply_id))
                model = None
                message = Message(chat_id=chat_id, msg_id=msg_id, user_id=user_id, is_me=False, text=text, model=model, reply_id=reply_id)
            else:
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
        else:
            if update.message.reply_to_message.from_user.id == self.bot_id:
                reply_id = reply_to_message.message_id
                await self.pending_pool.wait_for((chat_id, reply_id))
                model = None
                message = Message(chat_id=chat_id, msg_id=msg_id, user_id=user_id, is_me=False, text=text, model=model, reply_id=reply_id)
            else:
                return

        await message.add(self.database_session)
        history = await message.history(self.database_session)
        if history is None:
            self.logger.info("History broken")
            await self._send_message(chat_id, "history broken", msg_id)
            return
        model = typing.cast(str, history[0].model)

        async with MessageSender(self, chat_id, msg_id, model) as sender:
            template = await Template.get_template(self.database_session, chat_id, user_id)
            reply = ""
            async for delta in self.models[model](history, template):
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
        self.start = False
        self.first = True
        self.reply = ""

    async def __aenter__(self):
        return self

    async def _update(self, last=False):
        suffix = "" if last else " [!Generating...]"
        if self.first:
            self.first = False
            self.new_msg_id = await self.owner._send_message(self.chat_id, f"[{self.model}] " + self.reply + suffix, self.msg_id)
            self.owner.pending_pool.add((self.chat_id, self.new_msg_id))
        else:
            await self.owner._edit_message(self.chat_id, f"[{self.model}] " + self.reply + suffix, self.new_msg_id)

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
            self.target_time = current_time + 3

    async def __aexit__(self, *args):
        await self._update(last=True)
        self.owner.pending_pool.remove((self.chat_id, self.new_msg_id))


def prompt(template, model, owner):
    time = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")
    return template.format(model=model, owner=owner, time=time)


def anthropic(*, model: str, api_key: str, owner: str, proxy: str | None = None) -> Model:
    import anthropic

    aclient = anthropic.AsyncAnthropic(
        api_key=api_key,
        http_client=httpx.AsyncClient(proxy=proxy) if proxy is not None else None,
    )

    async def reply(history: list[Message], template: str) -> typing.AsyncGenerator[str, None]:
        messages = []
        for message in history:
            messages.append({"role": "assistant" if message.is_me else "user", "content": message.text})
        async with aclient.messages.stream(model=model, messages=messages, max_tokens=1024, system=prompt(template, model, owner)) as stream:
            async for text in stream.text_stream:
                yield text

    return reply


def gemini(*, model: str, api_key: str, owner: str, proxy: str | None = None) -> Model:

    async def reply(history: list[Message], template: str) -> typing.AsyncGenerator[str, None]:
        base_url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:streamGenerateContent"
        params = {'alt': 'sse', 'key': api_key}
        headers = {
            'Content-Type': 'application/json',
        }
        messages = []
        for message in history:
            messages.append({"role": "model" if message.is_me else "user", "parts": [{"text": message.text}]})
        payload = {
            "contents":
                messages,
            "systemInstruction": {
                "parts": [{
                    "text": prompt(template, model, owner)
                }]
            },
            "safetySettings": [{
                "category": category,
                "threshold": "BLOCK_NONE"
            } for category in [
                "HARM_CATEGORY_HARASSMENT",
                "HARM_CATEGORY_HATE_SPEECH",
                "HARM_CATEGORY_SEXUALLY_EXPLICIT",
                "HARM_CATEGORY_DANGEROUS_CONTENT",
                "HARM_CATEGORY_CIVIC_INTEGRITY",
            ]],
        }

        async with httpx.AsyncClient(proxy=proxy) as client:
            async with client.stream("POST", base_url, headers=headers, params=params, data=json.dumps(payload)) as response:
                async for chunk in response.aiter_text():
                    for line in chunk.splitlines():
                        if line.startswith("data:"):
                            data = line[len("data:"):].strip()
                            obj = json.loads(data)["candidates"][0]
                            if "content" in obj:
                                content = obj["content"]
                                if "role" in content:
                                    if content["role"] != "model":
                                        raise ValueError("Role error")
                                if "parts" in content:
                                    parts = content["parts"]
                                    assert len(parts) == 1
                                    part = parts[0]
                                    yield part["text"]
                            if "finishReason" in obj:
                                finish_reason = obj["finishReason"]
                                if finish_reason == "MAX_TOKENS":
                                    yield '\n\n[!] Error: Output truncated due to limit'
                                elif finish_reason == "STOP":
                                    pass
                                else:
                                    yield f'\n\n[!] Error: finish_reason="{finish_reason}"'
                                return

    return reply


def gpt(*, model: str, api_key: str, endpoint: str, owner: str, proxy: str | None = None, azure: bool = False) -> Model:
    import openai

    if azure:
        aclient = openai.AsyncAzureOpenAI(
            api_key=api_key,
            api_version="2024-02-15-preview",
            azure_endpoint=endpoint,
            http_client=httpx.AsyncClient(proxy=proxy) if proxy is not None else None,
            max_retries=0,
            timeout=15,
        )
    else:
        aclient = openai.AsyncOpenAI(
            api_key=api_key,
            base_url=endpoint,
            http_client=httpx.AsyncClient(proxy=proxy) if proxy is not None else None,
            max_retries=0,
            timeout=15,
        )

    async def reply(history: list[Message], template: str) -> typing.AsyncGenerator[str, None]:
        messages = [{"role": "system", "content": prompt(template, model, owner)}]
        for message in history:
            messages.append({"role": "assistant" if message.is_me else "user", "content": message.text})
        stream = await aclient.chat.completions.create(model=model, messages=messages, stream=True)
        async for response in stream:
            if response.choices:
                assert len(response.choices) == 1
                obj = response.choices[0]
                if obj.delta is not None:
                    if obj.delta.role is not None:
                        if obj.delta.role != "assistant":
                            raise ValueError("Role error")
                    if obj.delta.content is not None:
                        yield obj.delta.content
                if obj.finish_reason is not None:
                    assert obj.delta.content is None or obj.delta.content == ""
                    assert obj.delta.function_call is None
                    assert obj.delta.role is None or obj.delta.role == "assistant"
                    assert obj.delta.tool_calls is None
                    finish_reason = obj.finish_reason
                    if finish_reason == "length":
                        yield "\n\n[!] Error: Output truncated due to limit"
                    elif finish_reason == "stop":
                        pass
                    elif finish_reason is not None:
                        yield f'\n\n[!] Error: finish_reason="{finish_reason}"'
                    return

    return reply


if __name__ == "__main__":
    import sys
    import yaml

    config_file = sys.argv[1] if len(sys.argv) > 1 else "config.yml"

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s :: %(levelname)s :: %(message)s")
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.INFO)
    stdout_handler.setFormatter(formatter)
    logger.addHandler(stdout_handler)
    file_handler = logging.FileHandler(f"{__file__}.log")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    with open(config_file, "rt", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    app = BotApp(logger=logger, **config["app"])

    for model in config["models"]:
        app.prefixes[model["prefix"]] = model["name"]
        app.models[model["name"]] = globals()[model["type"]](**model["args"])

    asyncio.run(app.run())
