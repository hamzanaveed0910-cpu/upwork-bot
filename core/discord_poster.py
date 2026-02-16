import asyncio
import os
import discord
from discord.ext import commands
from core.logger import log
from core import db
from datetime import datetime
import io




class DiscordPoster:
    """Background Discord poster using discord.ext.commands.Bot.

    Provides runtime commands to manage tracked keywords via a ConfigManager instance.
    """

    def __init__(self, token: str, config_manager=None, command_prefix='!'):
        self.token = token
        intents = discord.Intents.default()
        intents.message_content = True
        self.bot = commands.Bot(command_prefix=command_prefix, intents=intents)
        self.queue: asyncio.Queue = asyncio.Queue()
        self._consumer_task: asyncio.Task | None = None
        self._started = False
        self.config_manager = config_manager

        def _is_job_relevant(job: dict, keyword: str) -> bool:
            """Check if job title or description contains the searched keyword."""
            if not keyword or not isinstance(job, dict):
                return True
            
            title = (job.get('title') or '').lower()
            description = (job.get('description') or '').lower()
            combined = f"{title} {description}"
            
            keywords = keyword.lower().split()
            keywords = [k for k in keywords if len(k) > 1]
            
            if not keywords:
                return True
            
            for kw in keywords:
                if kw in combined:
                    return True
            
            return False

        self._is_job_relevant = _is_job_relevant

        @self.bot.event
        async def on_ready():
            log.info(f"✅ Discord: Logged in as {self.bot.user}")

        # Register simple management commands
        @self.bot.command(name='track')
        async def _track(ctx, action: str = None, *args):
            # Usage: !track add <label> <#channel> <keyword>
            if action is None:
                await ctx.send("Usage: !track add|remove|list ...")
                return
            action = action.lower()
            # permission check: only guild managers or admins can modify tracked searches
            if action in ('add', 'remove', 'update', 'enable', 'disable'):
                perms = getattr(ctx.author, 'guild_permissions', None)
                if not (perms and (perms.manage_guild or perms.administrator)):
                    await ctx.send('You do not have permission to manage tracked searches. (Requires Manage Server or Administrator)')
                    return
            if action == 'list':
                tracked = self.config_manager.get_tracked() if self.config_manager else []
                if not tracked:
                    await ctx.send('No tracked searches configured.')
                    return
                lines = []
                for t in tracked:
                    lines.append(f"{t.get('label')} — `{t.get('keyword')}` → {t.get('channel_id')}")
                await ctx.send('\n'.join(lines))
                return

            if action == 'add':
                if len(args) < 3:
                    await ctx.send('Usage: !track add <label> <channel_id> <keyword>')
                    return
                label = args[0]
                try:
                    channel_id = int(args[1])
                except Exception:
                    await ctx.send('Invalid channel_id. Use a numeric channel ID.')
                    return
                # validate channel exists and is a text channel
                try:
                    channel = ctx.bot.get_channel(channel_id)
                    if channel is None:
                        channel = await ctx.bot.fetch_channel(channel_id)
                except Exception as e:
                    await ctx.send(f'Failed to fetch channel {channel_id}: {e}')
                    return
                if not hasattr(channel, 'send'):
                    await ctx.send('Provided channel_id is not a text channel. Provide a text channel ID (not a category).')
                    return
                keyword = ' '.join(args[2:])
                url = 'https://www.upwork.com/api/graphql/v1?alias=visitorJobSearch'
                if not self.config_manager:
                    await ctx.send('Config manager not available.')
                    return
                self.config_manager.add_tracked(url=url, label=label, keyword=keyword, channel_id=channel_id)
                await ctx.send(f'Added tracked search `{label}` for `{keyword}` → {channel_id} ({getattr(channel, "name", "channel")})')
                return

            if action == 'remove':
                if len(args) < 1:
                    await ctx.send('Usage: !track remove <label>')
                    return
                label = args[0]
                if not self.config_manager:
                    await ctx.send('Config manager not available.')
                    return
                ok = self.config_manager.remove_tracked_by_label(label)
                if ok:
                    await ctx.send(f'Removed tracked search `{label}`')
                else:
                    await ctx.send(f'No tracked search with label `{label}` found')
                return

            await ctx.send('Unknown action. Use add|remove|list')

        @self.bot.command(name='search')
        @commands.cooldown(1, 10, commands.BucketType.user)
        async def _search(ctx, *args):
            """Run a one-time search and post results to the channel.

            Usage:
              `!search teaching` — posts to current channel
              `!search 123456789012345678 teaching` — posts to given channel id
            """
            if not args:
                await ctx.send('Usage: !search [channel_id] <keyword> OR just `!search <keyword>` to save to this channel, or `!search --add <label> <channel_id> <keyword>`')
                return

            # Support: !search --add <label> <channel_id> <keyword>
            if args[0] in ('--add', '-a'):
                # Permission check
                perms = getattr(ctx.author, 'guild_permissions', None)
                if not (perms and (perms.manage_guild or perms.administrator)):
                    await ctx.send('You do not have permission to add tracked searches. (Requires Manage Server or Administrator)')
                    return
                if not self.config_manager:
                    await ctx.send('Config manager not available.')
                    return
                if len(args) < 4:
                    await ctx.send('Usage: !search --add <label> <channel_id> <keyword>')
                    return
                label = args[1]
                try:
                    add_channel_id = int(args[2])
                except Exception:
                    await ctx.send('Invalid channel_id. Use a numeric channel ID.')
                    return
                keyword = ' '.join(args[3:])
                # validate channel
                try:
                    ch = ctx.bot.get_channel(add_channel_id) or await ctx.bot.fetch_channel(add_channel_id)
                except Exception as e:
                    await ctx.send(f'Failed to fetch channel {add_channel_id}: {e}')
                    return
                if not hasattr(ch, 'send'):
                    await ctx.send('Provided channel_id is not a text channel. Provide a text channel ID (not a category).')
                    return
                url = 'https://www.upwork.com/api/graphql/v1?alias=visitorJobSearch'
                self.config_manager.add_tracked(url=url, label=label, keyword=keyword, channel_id=add_channel_id)
                await ctx.send(f'Added tracked search `{label}` for `{keyword}` → {add_channel_id} ({getattr(ch, "name", "channel")})')
                # run an immediate search and post results
                if hasattr(self, 'scraper') and self.scraper is not None:
                    await ctx.send(f"Running initial search for '{keyword}' and posting results to {getattr(ch, 'name', add_channel_id)}...")
                    try:
                        jobs, total, success = await self.scraper.fetch_jobs(keyword=keyword, urls=self.scraper.url)
                        if success and jobs:
                            posted = 0
                            for job in jobs[:5]:
                                title = job.get('title')
                                desc = job.get('description', '')[:300]
                                link = job.get('link')
                                body = f"**{title}**\n{desc}\n\nApply: {link}"
                                try:
                                    await ch.send(body)
                                    posted += 1
                                except Exception as e:
                                    await ctx.send(f'Failed to post result to target channel: {e}')
                                    break
                            await ctx.send(f'Posted {posted} initial result(s) for "{keyword}".')
                        else:
                            await ctx.send(f'No initial results for "{keyword}" or search failed.')
                    except Exception as e:
                        await ctx.send(f'Initial search failed: {e}')
                return

            # Determine if first arg is a channel id OR treat as keyword to save
            tokens = list(args)
            target_channel = ctx.channel

            # If first token is a channel id, pop it
            if tokens and tokens[0].isdigit() and len(tokens[0]) >= 17:
                try:
                    cid = int(tokens.pop(0))
                    target_channel = await self.bot.fetch_channel(cid)
                except Exception as e:
                    await ctx.send(f'Invalid channel id or cannot fetch channel: {e}')
                    return

            # Parse optional --urls / -u flag (accept comma-separated or space-separated URLs)
            urls = None
            if '--urls' in tokens or '-u' in tokens:
                flag = '--urls' if '--urls' in tokens else '-u'
                idx = tokens.index(flag)
                url_tokens = tokens[idx+1:]
                if not url_tokens:
                    await ctx.send('Provide one or more URLs after --urls')
                    return
                # combine remaining tokens as url strings, allow commas
                parsed_urls = []
                for t in url_tokens:
                    for part in t.split(','):
                        p = part.strip()
                        if p:
                            parsed_urls.append(p)
                urls = parsed_urls if parsed_urls else None
                # remove flag and url tokens from tokens so remaining tokens are keyword
                tokens = tokens[:idx]

            # Remaining tokens form the keyword(s)
            keyword_str = ' '.join(tokens).strip()
            if not keyword_str:
                await ctx.send('Provide a keyword to search. You can separate multiple fields with commas, e.g. `python, django`')
                return

            # Support multiple fields separated by comma
            fields = [f.strip() for f in keyword_str.split(',') if f.strip()]
            if not fields:
                await ctx.send('No valid search fields provided.')
                return

            if not hasattr(self, 'scraper') or self.scraper is None:
                await ctx.send('Scraper not available on this bot instance.')
                return

            # Permission check for adding tracked searches
            perms = getattr(ctx.author, 'guild_permissions', None)
            if not (perms and (perms.manage_guild or perms.administrator)):
                await ctx.send('You do not have permission to add tracked searches. (Requires Manage Server or Administrator)')
                return

            if not self.config_manager:
                await ctx.send('Config manager not available.')
                return

            # Derive a label from the first field and ensure uniqueness
            base_label = fields[0].strip().lower().replace(' ', '-')[:50]
            label = base_label
            i = 1
            existing = {t.get('label') for t in self.config_manager.get_tracked()}
            while label in existing:
                i += 1
                label = f"{base_label}-{i}"

            add_channel_id = getattr(target_channel, 'id', None)
            if add_channel_id is None:
                await ctx.send('Target channel is not valid.')
                return

            # Validate target channel is text
            if not hasattr(target_channel, 'send'):
                await ctx.send('Target channel is not a text channel. Use a text channel.')
                return

            # Store the URL(s) in the config entry; use the provided urls or default scraper url
            entry_url = urls if urls else self.scraper.url
            # For keyword, store the full keyword string
            keyword = keyword_str
            self.config_manager.add_tracked(url=entry_url, label=label, keyword=keyword, channel_id=add_channel_id)
            await ctx.send(f'Added tracked search `{label}` for `{keyword}` → {add_channel_id} ({getattr(target_channel, "name", add_channel_id)})')

            # Run initial searches across fields and urls concurrently
            await ctx.send(f"Running initial search for '{keyword}' and posting results to {getattr(target_channel, 'name', add_channel_id)}...")
            try:
                # Collect jobs from each field, allowing multi-url fetch inside fetch_jobs
                seen = {}
                import asyncio as _aio
                fetch_tasks = [_aio.create_task(self.scraper.fetch_jobs(keyword=fld, urls=entry_url)) for fld in fields]
                results = await _aio.gather(*fetch_tasks)
                combined = []
                for jobs, total, success in results:
                    if jobs:
                        for job in jobs:
                            jid = job.get('id')
                            if jid and jid not in seen:
                                seen[jid] = job
                                combined.append(job)

                if combined:
                    posted = 0
                    for job in combined[:5]:
                        # Apply keyword relevance check
                        if not self._is_job_relevant(job, keyword_str):
                            continue
                        
                        # Build full job details for thread
                        title = job.get('title')
                        job_type = job.get('jobType', 'N/A')
                        budget = job.get('budget', {})
                        budget_str = 'N/A'
                        if isinstance(budget, dict):
                            if 'amount' in budget:
                                budget_str = f"{budget['amount']} {budget.get('isoCurrencyCode', 'USD')}"
                            elif 'min' in budget and 'max' in budget:
                                budget_str = f"${budget['min']}-${budget['max']}/hr"
                        
                        # Format published time
                        from datetime import datetime, timezone
                        published_raw = job.get('published')
                        published_str = 'N/A'
                        try:
                            local_tz = datetime.now().astimezone().tzinfo
                            if isinstance(published_raw, (int, float)):
                                ts = float(published_raw)
                                if ts > 1e12:
                                    ts = ts / 1000.0
                                dt = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(local_tz)
                                published_str = dt.strftime('%Y-%m-%d %H:%M %Z')
                            elif published_raw:
                                s = str(published_raw)
                                if 'T' in s:
                                    s2 = s.rstrip('Z')
                                    try:
                                        dt = datetime.fromisoformat(s2)
                                        if dt.tzinfo is None:
                                            dt = dt.replace(tzinfo=timezone.utc)
                                        dt = dt.astimezone(local_tz)
                                        published_str = dt.strftime('%Y-%m-%d %H:%M %Z')
                                    except:
                                        published_str = s
                                else:
                                    try:
                                        ts = float(s)
                                        if ts > 1e12:
                                            ts = ts / 1000.0
                                        dt = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(local_tz)
                                        published_str = dt.strftime('%Y-%m-%d %H:%M %Z')
                                    except:
                                        published_str = s
                        except:
                            pass
                        
                        # Truncate description to avoid bloated files (keep max 5000 chars with truncation note)
                        desc = job.get('description', 'N/A')
                        if isinstance(desc, str) and len(desc) > 5000:
                            desc = desc[:5000] + "\n\n[Description truncated due to length limit]"
                        
                        full_details = f"""**Full Job Details**
**Title:** {title}
**Type:** {job_type}
**Budget:** {budget_str}
**Published:** {published_str}
**Description:** {desc}
**Link:** {job['link']}"""
                        
                        short_desc = f"Posted: {published_str}\n{job.get('description','')[:300]}"
                        
                        try:
                            await self.enqueue_job({
                                'title': title,
                                'description': short_desc,
                                'link': job['link'],
                                'full_details': full_details
                            }, add_channel_id)
                            posted += 1
                        except Exception as e:
                            await ctx.send(f'Failed to enqueue job to channel: {e}')
                            break
                    
                    await ctx.send(f'Posted {posted} initial result(s) for "{keyword}" with full details.')
                else:
                    await ctx.send(f'No initial results for "{keyword}" or search failed.')
            except Exception as e:
                await ctx.send(f'Initial search failed: {e}')

        @self.bot.command(name='delete', aliases=['remove'])
        @commands.cooldown(1, 5, commands.BucketType.user)
        async def _delete(ctx, label: str = None):
            if not label:
                await ctx.send('Usage: !delete <tracked-label>')
                return

            perms = getattr(ctx.author, 'guild_permissions', None)
            if not (perms and (perms.manage_guild or perms.administrator)):
                await ctx.send('You do not have permission to remove tracked searches. (Requires Manage Server or Administrator)')
                return

            if not self.config_manager:
                await ctx.send('Config manager not available.')
                return

            try:
                removed = self.config_manager.remove_tracked_by_label(label)
            except Exception:
                removed = False

            if removed:
                await ctx.send(f'Removed tracked search `{label}` from config.')
            else:
                await ctx.send(f'No tracked search found with label `{label}`.')

        @self.bot.command(name='status')
        async def _status(ctx):
            # Admin-only status: show last-post time per tracked label
            perms = getattr(ctx.author, 'guild_permissions', None)
            if not (perms and (perms.manage_guild or perms.administrator)):
                await ctx.send('You do not have permission to view status. (Requires Manage Server or Administrator)')
                return

            tracked = self.config_manager.get_tracked() if self.config_manager else []
            # Map label -> last_seen
            try:
                rows = await db.async_get_last_post_times()
            except Exception as e:
                await ctx.send(f'Failed to query DB for status: {e}')
                return

            last_map = {r[0]: r[1] for r in rows}

            lines = []
            for t in tracked:
                label = t.get('label')
                last = last_map.get(label)
                if last:
                    # try to parse sqlite datetime string and present it
                    try:
                        dt = datetime.fromisoformat(last)
                        display = dt.strftime('%Y-%m-%d %H:%M:%S')
                    except Exception:
                        display = str(last)
                else:
                    display = 'Never'
                lines.append(f"{label}: {display}")

            if not lines:
                await ctx.send('No tracked searches configured.')
                return

            # Send as a single message
            await ctx.send('Tracked search status:\n' + '\n'.join(lines))

    async def _consumer(self):
        await asyncio.sleep(0.5)
        while True:
            job_payload = await self.queue.get()
            try:
                channel_id = int(job_payload.get('channel_id'))
                log.info(f"Discord: Attempting to post to channel {channel_id}")
                channel = self.bot.get_channel(channel_id)
                if channel is None:
                    log.info(f"Discord: Channel {channel_id} not in cache, fetching...")
                    try:
                        channel = await self.bot.fetch_channel(channel_id)
                        log.info(f"Discord: Fetched channel {channel_id}: {getattr(channel, 'name', repr(channel))}")
                    except Exception as e:
                        log.error(f"Discord: Failed to fetch channel {channel_id}: {e}")
                        # If the channel is gone (404), remove tracked entries referencing it
                        try:
                            if isinstance(e, discord.NotFound) or (hasattr(e, 'status') and getattr(e, 'status') == 404):
                                if hasattr(self, 'config_manager') and self.config_manager:
                                    removed = self.config_manager.remove_tracked_by_channel(channel_id)
                                    if removed:
                                        log.info(f"Discord: Removed {removed} tracked entries referencing missing channel {channel_id}")
                        except Exception:
                            pass
                        continue

                # Verify it's a text channel
                if not hasattr(channel, 'send'):
                    log.error(f"Discord: Channel {channel_id} is not a text channel (may be a category). Use a text channel ID instead.")
                    try:
                        await channel.send('')
                    except Exception:
                        pass
                    continue

                title = job_payload.get('title')
                link = job_payload.get('link')
                desc = job_payload.get('description', '')
                body = f"**{title}**\n{desc}\n\nApply: {link}"

                log.info(f"Discord: Sending message to {getattr(channel, 'name', channel_id)}...")
                msg = await channel.send(body)
                log.info(f"Discord: Message sent successfully to {getattr(channel, 'name', channel_id)}")

                full = job_payload.get('full_details')
                if full:
                    try:
                        log.info(f"Discord: Creating thread for {title[:50]}...")
                        # Discord thread name limit is 100 characters
                        # "Job: " is 5 chars, so we have 95 chars for the title
                        thread_name = f"Job: {title[:95]}"
                        # Ensure it doesn't exceed 100 chars (account for any edge cases)
                        if len(thread_name) > 100:
                            thread_name = thread_name[:97] + "..."
                        thread = await msg.create_thread(name=thread_name, auto_archive_duration=60)
                        # Discord message content limit is 2000 characters. If the full details
                        # exceed that, send a short preview and upload the full details as a .txt file.
                        try:
                            if len(full) <= 1900:
                                await thread.send(full)
                            else:
                                preview = full[:1900] + "\n\n(Full details truncated; uploading as a file below)"
                                await thread.send(preview)
                                bio = io.BytesIO(full.encode('utf-8'))
                                bio.seek(0)
                                file_size_kb = len(full) / 1024
                                log.info(f"Discord: Uploading job details as file ({file_size_kb:.1f} KB) for job {job_payload.get('id', 'unknown')}")
                                await thread.send(file=discord.File(fp=bio, filename=f"job-{job_payload.get('id', 'details')}.txt"))
                                log.info(f"Discord: File uploaded successfully to thread")
                        except Exception as send_exc:
                            log.error(f"Discord: Failed to send full details in thread for {title}: {send_exc}", exc_info=True)
                        log.info(f"Discord: Thread created successfully")
                    except Exception as e:
                        log.warning(f"Discord: Failed to create thread for {title}: {e}")

            except Exception as e:
                log.error(f"Discord: error posting job: {e}", exc_info=True)
            finally:
                try:
                    self.queue.task_done()
                except Exception:
                    pass

    async def start(self):
        if self._started:
            return
        loop = asyncio.get_running_loop()
        self._consumer_task = loop.create_task(self._consumer())
        loop.create_task(self.bot.start(self.token))
        self._started = True

    async def stop(self):
        try:
            if not self.bot.is_closed():
                await self.bot.close()
        except Exception:
            pass
        if self._consumer_task:
            try:
                self._consumer_task.cancel()
            except Exception:
                pass

    async def enqueue_job(self, payload: dict, channel_id: int):
        payload = dict(payload)
        payload['channel_id'] = channel_id
        await self.queue.put(payload)
