import json
import asyncio
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder

TOKEN="BOT_TOKEN"

DATA_FILE="data.json"

bot=Bot(TOKEN)
dp=Dispatcher()

state={}

# ---------- LOAD DATA ----------

def load():

    with open(DATA_FILE,"r",encoding="utf8") as f:
        return json.load(f)

def save():

    with open(DATA_FILE,"w",encoding="utf8") as f:
        json.dump(data,f,ensure_ascii=False,indent=2)

data=load()

# ---------- TEXT ----------

def company_text(chat_id,name):

    tasks=data["workspaces"][str(chat_id)]["companies"][name]["tasks"]

    lines=[f"📁 Клиент: {name}"]

    for i,t in enumerate(tasks,1):

        icon="✅" if t["done"] else "⬜"

        txt=t["text"]

        if t["done"]:
            txt=f"~~{txt}~~"

        lines.append(f"{i}. {icon} {txt}")

    return "\n".join(lines)

# ---------- KEYBOARDS ----------

def companies_menu(chat_id):

    kb=InlineKeyboardBuilder()

    companies=data["workspaces"][str(chat_id)]["companies"]

    for c in companies:
        kb.button(text=c,callback_data=f"open:{chat_id}:{c}")

    kb.button(text="➕ Новая компания",callback_data=f"new:{chat_id}")

    kb.adjust(1)

    return kb.as_markup()

def tasks_menu(chat_id,name):

    kb=InlineKeyboardBuilder()

    tasks=data["workspaces"][str(chat_id)]["companies"][name]["tasks"]

    for i,t in enumerate(tasks):

        kb.button(
            text=f"{i+1} {t['text']}",
            callback_data=f"task:{chat_id}:{name}:{i}"
        )

    kb.button(text="➕ Добавить задачу",callback_data=f"add:{chat_id}:{name}")
    kb.button(text="🗑 Удалить список",callback_data=f"delete:{chat_id}:{name}")
    kb.button(text="🔙 Назад",callback_data=f"back:{chat_id}")

    kb.adjust(1)

    return kb.as_markup()

def task_menu(chat_id,name,i):

    kb=InlineKeyboardBuilder()

    kb.button(text="✔ Выполнить",callback_data=f"done:{chat_id}:{name}:{i}")
    kb.button(text="🟡 Снять выполнение",callback_data=f"undone:{chat_id}:{name}:{i}")

    kb.button(text="✏ Переименовать",callback_data=f"rename:{chat_id}:{name}:{i}")
    kb.button(text="❌ Удалить",callback_data=f"remove:{chat_id}:{name}:{i}")

    kb.button(text="🔙 Назад",callback_data=f"open:{chat_id}:{name}")

    kb.adjust(2)

    return kb.as_markup()

# ---------- UPDATE ----------

async def update_company(chat_id,name):

    workspace=data["workspaces"][str(chat_id)]

    msg_id=workspace["companies"][name]["message_id"]

    await bot.edit_message_text(
        company_text(chat_id,name),
        chat_id=chat_id,
        message_id=msg_id,
        reply_markup=tasks_menu(chat_id,name)
    )

# ---------- SETUP ----------

@dp.message(Command("start"))
async def start(m:types.Message):

    if m.chat.type=="private":

        state[m.from_user.id]="setup"

        await m.answer("Перешлите сообщение из нужного треда")

        return

    chat_id=m.chat.id

    if str(chat_id) not in data["workspaces"]:
        return

    workspace=data["workspaces"][str(chat_id)]

    if workspace["mode"]=="duplicate":
        return

    await m.answer(
        "📋 Компании",
        reply_markup=companies_menu(chat_id)
    )

# ---------- SETUP HANDLER ----------

@dp.message()
async def setup_handler(m:types.Message):

    s=state.get(m.from_user.id)

    if s!="setup":
        return

    if not m.forward_from_chat:

        await m.answer("Перешлите сообщение из чата")

        return

    chat_id=m.forward_from_chat.id
    thread_id=m.message_thread_id

    data["workspaces"][str(chat_id)]={

        "thread_id":thread_id,
        "mode":"main",

        "template":[
            "Создать договор",
            "Выставить счет",
            "Подготовить мебель"
        ],

        "companies":{}
    }

    save()

    await m.answer(
"""
Чат подключен

Добавьте бота в этот чат
и напишите /start в нужном треде
"""
    )

    state.pop(m.from_user.id)

# ---------- NEW COMPANY ----------

@dp.callback_query(F.data.startswith("new"))
async def new_company(c):

    _,chat_id=c.data.split(":")

    state[c.from_user.id]=f"company:{chat_id}"

    await c.message.answer("Введите название компании")

# ---------- COMPANY INPUT ----------

@dp.message()
async def company_input(m:types.Message):

    s=state.get(m.from_user.id)

    if not s or not s.startswith("company"):
        return

    _,chat_id=s.split(":")

    workspace=data["workspaces"][chat_id]

    name=m.text

    tasks=[{"text":t,"done":False} for t in workspace["template"]]

    msg=await bot.send_message(
        chat_id,
        company_text(chat_id,name),
        message_thread_id=workspace["thread_id"],
        reply_markup=tasks_menu(chat_id,name)
    )

    workspace["companies"][name]={
        "tasks":tasks,
        "message_id":msg.message_id
    }

    save()

    await m.delete()

    state.pop(m.from_user.id)

# ---------- RUN ----------

async def main():
    await dp.start_polling(bot)

if __name__=="__main__":
    asyncio.run(main())
