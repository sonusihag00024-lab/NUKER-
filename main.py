import discord
from discord.ext import commands
import asyncio

# ---------- CONFIG ----------
TRIGGER = ""                     # Trigger word
ASSIGN_TO_IDS = []               # Optional: list of user IDs to assign role to
MASS_ASSIGN = False              # True to give role to all members
IGNORED_IDS = []                 # List of member IDs to ignore
ROLE_ID = 123456789012345678     # Role ID to assign
GUILD_ID = 123456789098765       # Server ID
BATCH_DELAY = 0.5                # Delay between role assignments to avoid rate limits
# ----------------------------

intents = discord.Intents.default()
intents.guilds = True
intents.members = True

bot = commands.Bot(command_prefix="", intents=intents)

async def assign_role_to_member(member, role):
    if member.id in IGNORED_IDS:
        print(f"Skipping ignored member {member.name}")
        return

    try:
        await member.add_roles(role)
        print(f"Gave role {role.name} to {member.name}")
    except discord.Forbidden:
        print(f"No permission to give role {role.name} to {member.name}")
    except discord.HTTPException as e:
        print(f"Failed to give role {role.name} to {member.name}: {e}")

@bot.event
async def on_ready():
    print(f"Logged in as {bot.user}")

@bot.event
async def on_message(message):
    if message.author.bot:
        return

    if message.content.lower().strip() == TRIGGER.lower():
        guild = bot.get_guild(GUILD_ID)
        if not guild:
            print("Bot is not in the specified guild.")
            return

        role = guild.get_role(ROLE_ID)
        if not role:
            print("Role not found.")
            return

        # Assign to specific users
        for user_id in ASSIGN_TO_IDS:
            member = await guild.fetch_member(user_id)
            if member:
                await assign_role_to_member(member, role)
                await asyncio.sleep(BATCH_DELAY)

        # Assign to all members
        if MASS_ASSIGN:
            async for member in guild.fetch_members(limit=None):
                if member.bot:
                    continue
                await assign_role_to_member(member, role)
                await asyncio.sleep(BATCH_DELAY)

bot.run("YOUR_BOT_TOKEN_HERE")
