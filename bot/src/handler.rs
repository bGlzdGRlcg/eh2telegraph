use std::{borrow::Cow, collections::HashSet, sync::Arc};

use eh2telegraph::{
    collector::{e_hentai::EHCollector, exhentai::EXCollector, nhentai::NHCollector},
    config::{self, WhitelistConfig}, // Add whitelist
    searcher::{
        f_hash::FHashConvertor,
        saucenao::{SaucenaoOutput, SaucenaoParsed, SaucenaoSearcher},
        ImageSearcher,
    },
    storage::KVStorage,
    sync::Synchronizer,
};

use reqwest::Url;
use std::collections::HashMap;
use std::sync::Mutex;
use teloxide::{
    adaptors::DefaultParseMode,
    prelude::*,
    utils::{
        command::BotCommands,
        markdown::{code_inline, escape, link},
    },
};
use tokio::sync::oneshot;
use tracing::{info, trace};

use crate::{ok_or_break, util::PrettyChat};

const MIN_SIMILARITY: u8 = 70;
const MIN_SIMILARITY_PRIVATE: u8 = 50;

#[derive(BotCommands, Clone)]
#[command(
    rename_rule = "lowercase",
    description = "\
    This is a gallery synchronization robot that is convenient for users to view pictures directly in Telegram.\n\
    这是一个方便用户直接在 Telegram 里看图的画廊同步机器人。\n\
    Bot supports sync with command, text url, or image(private chat search thrashold is lower).\n\
    机器人支持通过 命令、直接发送链接、图片(私聊搜索相似度阈值会更低) 的形式同步。\n\n\
    Bot develop group / Bot 开发群 https://t.me/TGSyncBotWorkGroup\n\
    And welcome to join image channel / 频道推荐 https://t.me/sesecollection\n\n\
    These commands are supported:\n\
    目前支持这些指令:"
)]
pub enum Command {
    #[command(description = "Display this help. 显示这条帮助信息")]
    Help,
    #[command(description = "Show bot verison. 显示机器人版本")]
    Version,
    #[command(description = "Show your account id. 显示你的账号 ID")]
    Id,
    #[command(
        description = "Sync a gallery(e-hentai/exhentai/nhentai are supported now). 同步一个画廊(目前支持 EH/EX/NH)"
    )]
    Sync(String),
    #[command(description = "Cancel all ongoing sync operations. 取消所有正在进行的同步操作。")]
    Cancel,
}

#[derive(BotCommands, Clone)]
#[command(rename_rule = "lowercase", description = "Command for admins")]
pub enum AdminCommand {
    #[command(description = "Delete cache with given key.")]
    Delete(String),
}

pub struct Handler<C> {
    pub synchronizer: Synchronizer<C>,
    pub searcher: SaucenaoSearcher,
    pub convertor: FHashConvertor,
    pub admins: HashSet<i64>,
    pub whitelist: HashSet<i64>, // Add whitelist

    single_flight: singleflight_async::SingleFlight<String>,

    // One user can have multiple active syncs
    active_syncs: Arc<Mutex<HashMap<i64, Vec<(String, oneshot::Sender<()>)>>>>,
}

impl<C> Handler<C>
where
    C: KVStorage<String> + Send + Sync + 'static,
{
    pub fn new(synchronizer: Synchronizer<C>, admins: HashSet<i64>) -> Self {
        // Read whitelist ids
        let whitelist = match config::parse::<WhitelistConfig>("whitelist")
            .ok()
            .and_then(|x| x)
        {
            Some(config) => {
                if config.enabled {
                    // use ids in config
                    config.ids.into_iter().collect()
                } else {
                    // Allow all ppl to use
                    HashSet::from([i64::MIN])
                }
            }
            None => {
                // No whitelist, all ppl can use
                HashSet::from([i64::MIN])
            }
        };

        Self {
            synchronizer,
            searcher: SaucenaoSearcher::new_from_config(),
            convertor: FHashConvertor::new_from_config(),
            admins,
            whitelist,
            single_flight: Default::default(),
            active_syncs: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    // Whitelist is allowed to use
    fn is_allowed(&self, chat_id: i64) -> bool {
        if self.admins.contains(&chat_id) {
            return true;
        }
        if self.whitelist.contains(&i64::MIN) {
            return true;
        }
        self.whitelist.contains(&chat_id)
    }

    // Support Multiple Sync Task
    fn register_sync(&self, user_id: i64, url: &str) -> oneshot::Receiver<()> {
        let (tx, rx) = oneshot::channel();

        let mut active_syncs = self.active_syncs.lock().unwrap();

        let user_syncs = active_syncs.entry(user_id).or_insert_with(Vec::new);

        user_syncs.push((url.to_string(), tx));

        rx
    }

    fn unregister_sync(&self, user_id: i64, url: &str) {
        let mut active_syncs = self.active_syncs.lock().unwrap();

        if active_syncs.contains_key(&user_id) {
            let should_remove_entry = {
                let user_syncs = active_syncs.get_mut(&user_id).unwrap();

                user_syncs.retain(|(sync_url, _)| sync_url != url);

                user_syncs.is_empty()
            };

            if should_remove_entry {
                active_syncs.remove(&user_id);
            }
        }
    }

    fn cancel_all_syncs(&self, user_id: i64) -> usize {
        let mut active_syncs = self.active_syncs.lock().unwrap();

        if let Some(user_syncs) = active_syncs.remove(&user_id) {
            let count = user_syncs.len();

            // Send all cancellation signal
            for (url, tx) in user_syncs {
                info!(
                    "[cancel handler] cancelling sync for user {} and url {}",
                    user_id, url
                );
                let _ = tx.send(());
            }

            info!(
                "[cancel handler] user {} cancelled {} sync operations",
                user_id, count
            );

            count
        } else {
            0
        }
    }

    // Add unauthorized response
    async fn send_unauthorized(&self, bot: &DefaultParseMode<Bot>, msg: &Message) {
        // Only send in PM
        if msg.chat.is_private() {
            let _ = bot
                .send_message(msg.chat.id, escape("User not authorized!"))
                .reply_to_message_id(msg.id)
                .await;
        }
    }

    /// Executed when a command comes in and parsed successfully.
    pub async fn respond_cmd(
        &'static self,
        bot: DefaultParseMode<Bot>,
        msg: Message,
        command: Command,
    ) -> ControlFlow<()> {
        match command {
            Command::Help => {
                let _ = bot
                    .send_message(msg.chat.id, escape(&Command::descriptions().to_string()))
                    .reply_to_message_id(msg.id)
                    .await;
            }
            Command::Version => {
                let _ = bot
                    .send_message(msg.chat.id, escape(crate::version::VERSION))
                    .reply_to_message_id(msg.id)
                    .await;
            }
            Command::Id => {
                let _ = bot
                    .send_message(
                        msg.chat.id,
                        format!(
                            "Current chat id is {} \\(in private chat this is your account id\\)",
                            code_inline(&msg.chat.id.to_string())
                        ),
                    )
                    .reply_to_message_id(msg.id)
                    .await;
            }
            Command::Sync(url) => {
                // Add white list check
                if !self.is_allowed(msg.chat.id.0) {
                    self.send_unauthorized(&bot, &msg).await;
                    return ControlFlow::Break(());
                }
                if url.is_empty() {
                    let _ = bot
                        .send_message(msg.chat.id, escape("Usage: /sync url"))
                        .reply_to_message_id(msg.id)
                        .await;
                    return ControlFlow::Break(());
                }

                info!(
                    "[cmd handler] receive sync request from {:?} for {url}",
                    PrettyChat(&msg.chat)
                );
                let msg: Message = ok_or_break!(
                    bot.send_message(msg.chat.id, escape(&format!("Syncing url {url}")))
                        .reply_to_message_id(msg.id)
                        .await
                );

                let url_clone = url.clone();
                let cancel_rx = self.register_sync(msg.chat.id.0, &url);

                tokio::spawn(async move {
                    let result = self.sync_response(&url, cancel_rx).await;

                    self.unregister_sync(msg.chat.id.0, &url_clone);

                    let _ = bot.edit_message_text(msg.chat.id, msg.id, result).await;
                });
            }
            Command::Cancel => {
                let cancelled_count = self.cancel_all_syncs(msg.chat.id.0);
                if cancelled_count > 0 {
                    let _ = bot
                        .send_message(
                            msg.chat.id,
                            escape(&format!("Cancelled {} sync operations.", cancelled_count)),
                        )
                        .reply_to_message_id(msg.id)
                        .await;
                } else {
                    let _ = bot
                        .send_message(msg.chat.id, escape("No active sync operations to cancel."))
                        .reply_to_message_id(msg.id)
                        .await;
                }
            }
        };

        ControlFlow::Break(())
    }

    pub async fn respond_admin_cmd(
        &'static self,
        bot: DefaultParseMode<Bot>,
        msg: Message,
        command: AdminCommand,
    ) -> ControlFlow<()> {
        match command {
            AdminCommand::Delete(key) => {
                tokio::spawn(async move {
                    let _ = self.synchronizer.delete_cache(&key).await;
                    let _ = bot
                        .send_message(msg.chat.id, escape(&format!("Key {key} deleted.")))
                        .reply_to_message_id(msg.id)
                        .await;
                });
                ControlFlow::Break(())
            }
        }
    }

    pub async fn respond_text(
        &'static self,
        bot: DefaultParseMode<Bot>,
        msg: Message,
    ) -> ControlFlow<()> {
        // Add white list check
        if !self.is_allowed(msg.chat.id.0) {
            self.send_unauthorized(&bot, &msg).await;
            return ControlFlow::Break(());
        }
        let maybe_link = {
            let entries = msg
                .entities()
                .map(|es| {
                    es.iter().filter_map(|e| {
                        if let teloxide::types::MessageEntityKind::TextLink { url } = &e.kind {
                            Synchronizer::match_url_from_text(url.as_ref()).map(ToOwned::to_owned)
                        } else {
                            None
                        }
                    })
                })
                .into_iter()
                .flatten();
            msg.text()
                .and_then(|content| {
                    Synchronizer::match_url_from_text(content).map(ToOwned::to_owned)
                })
                .into_iter()
                .chain(entries)
                .next()
        };

        if let Some(url) = maybe_link {
            info!(
                "[text handler] receive sync request from {:?} for {url}",
                PrettyChat(&msg.chat)
            );
            let msg: Message = ok_or_break!(
                bot.send_message(msg.chat.id, escape(&format!("Syncing url {url}")))
                    .reply_to_message_id(msg.id)
                    .await
            );

            let url_clone = url.clone();
            let cancel_rx = self.register_sync(msg.chat.id.0, &url);

            tokio::spawn(async move {
                let result = self.sync_response(&url, cancel_rx).await;

                self.unregister_sync(msg.chat.id.0, &url_clone);

                let _ = bot.edit_message_text(msg.chat.id, msg.id, result).await;
            });
            return ControlFlow::Break(());
        }

        // fallback to the next branch
        ControlFlow::Continue(())
    }

    pub async fn respond_caption(
        &'static self,
        bot: DefaultParseMode<Bot>,
        msg: Message,
    ) -> ControlFlow<()> {
        // Add white list check
        if !self.is_allowed(msg.chat.id.0) {
            self.send_unauthorized(&bot, &msg).await;
            return ControlFlow::Break(());
        }
        let caption_entities = msg.caption_entities();
        let mut final_url = None;
        for entry in caption_entities.map(|x| x.iter()).into_iter().flatten() {
            let url = match &entry.kind {
                teloxide::types::MessageEntityKind::Url => {
                    let raw = msg
                        .caption()
                        .expect("Url MessageEntry found but caption is None");
                    let encoded: Vec<_> = raw
                        .encode_utf16()
                        .skip(entry.offset)
                        .take(entry.length)
                        .collect();
                    let content = ok_or_break!(String::from_utf16(&encoded));
                    Cow::from(content)
                }
                teloxide::types::MessageEntityKind::TextLink { url } => Cow::from(url.as_ref()),
                _ => {
                    continue;
                }
            };
            let url = if let Some(c) = Synchronizer::match_url_from_url(&url) {
                c
            } else {
                continue;
            };
            final_url = Some(url.to_string());
            break;
        }

        match final_url {
            Some(url) => {
                info!(
                    "[caption handler] receive sync request from {:?} for {url}",
                    PrettyChat(&msg.chat)
                );
                let msg: Message = ok_or_break!(
                    bot.send_message(msg.chat.id, escape(&format!("Syncing url {url}")))
                        .reply_to_message_id(msg.id)
                        .await
                );

                let url_clone = url.clone();
                let cancel_rx = self.register_sync(msg.chat.id.0, &url);

                tokio::spawn(async move {
                    let result = self.sync_response(&url, cancel_rx).await;

                    self.unregister_sync(msg.chat.id.0, &url_clone);

                    let _ = bot.edit_message_text(msg.chat.id, msg.id, result).await;
                });
                ControlFlow::Break(())
            }
            None => ControlFlow::Continue(()),
        }
    }

    pub async fn respond_photo(
        &'static self,
        bot: DefaultParseMode<Bot>,
        msg: Message,
    ) -> ControlFlow<()> {
        // Add white list check
        if !self.is_allowed(msg.chat.id.0) {
            self.send_unauthorized(&bot, &msg).await;
            return ControlFlow::Break(());
        }
        let first_photo = match msg.photo().and_then(|x| x.first()) {
            Some(p) => p,
            None => {
                return ControlFlow::Continue(());
            }
        };

        let f = ok_or_break!(bot.get_file(&first_photo.file.id).await);
        let mut buf: Vec<u8> = Vec::with_capacity(f.size as usize);
        ok_or_break!(teloxide::net::Download::download_file(&bot, &f.path, &mut buf).await);
        let search_result: SaucenaoOutput = ok_or_break!(self.searcher.search(buf).await);

        let mut url_sim = None;
        let threshold = if msg.chat.is_private() {
            MIN_SIMILARITY_PRIVATE
        } else {
            MIN_SIMILARITY
        };
        for element in search_result
            .data
            .into_iter()
            .filter(|x| x.similarity >= threshold)
        {
            match element.parsed {
                SaucenaoParsed::EHentai(f_hash) => {
                    url_sim = Some((
                        ok_or_break!(self.convertor.convert_to_gallery(&f_hash).await),
                        element.similarity,
                    ));
                    break;
                }
                SaucenaoParsed::NHentai(nid) => {
                    url_sim = Some((format!("https://nhentai.net/g/{nid}/"), element.similarity));
                    break;
                }
                _ => continue,
            }
        }

        let (url, sim) = match url_sim {
            Some(u) => u,
            None => {
                trace!("[photo handler] image not found");
                return ControlFlow::Continue(());
            }
        };

        info!(
            "[photo handler] receive sync request from {:?} for {url} with similarity {sim}",
            PrettyChat(&msg.chat)
        );

        if let Ok(msg) = bot
            .send_message(msg.chat.id, escape(&format!("Syncing url {url}")))
            .reply_to_message_id(msg.id)
            .await
        {
            let url_clone = url.clone();
            let cancel_rx = self.register_sync(msg.chat.id.0, &url);

            tokio::spawn(async move {
                let result = self.sync_response(&url, cancel_rx).await;

                self.unregister_sync(msg.chat.id.0, &url_clone);

                let _ = bot.edit_message_text(msg.chat.id, msg.id, result).await;
            });
        }

        ControlFlow::Break(())
    }

    pub async fn respond_default(
        &'static self,
        bot: DefaultParseMode<Bot>,
        msg: Message,
    ) -> ControlFlow<()> {
        if msg.chat.is_private() {
            ok_or_break!(
                bot.send_message(msg.chat.id, escape("Unrecognized message. Maybe /help ?"))
                    .reply_to_message_id(msg.id)
                    .await
            );
        }
        #[cfg(debug_assertions)]
        tracing::warn!("{:?}", msg);
        ControlFlow::Break(())
    }

    // Updated sync_response method with cancellation
    async fn sync_response(&self, url: &str, mut cancel_rx: oneshot::Receiver<()>) -> String {
        tokio::select! {
            result = self.single_flight.work(url, || async {
                match self.route_sync(url).await {
                    Ok(sync_url) => {
                        format!("Sync to telegraph finished: {}", link(&sync_url, &escape(&sync_url)))
                    }
                    Err(e) => {
                        format!("Sync to telegraph failed: {}", escape(&e.to_string()))
                    }
                }
            }) => result,
            _ = &mut cancel_rx => {
                "Sync operation was cancelled.".to_string()
            }
        }
    }

    async fn route_sync(&self, url: &str) -> anyhow::Result<String> {
        let u = Url::parse(url).map_err(|_| anyhow::anyhow!("Invalid url"))?;
        let host = u.host_str().unwrap_or_default();
        let path = u.path().to_string();

        // TODO: use macro to generate them
        #[allow(clippy::single_match)]
        match host {
            "e-hentai.org" => {
                info!("[registry] sync e-hentai for path {}", path);
                self.synchronizer
                    .sync::<EHCollector>(path)
                    .await
                    .map_err(anyhow::Error::from)
            }
            "nhentai.to" | "nhentai.net" => {
                info!("[registry] sync nhentai for path {}", path);
                self.synchronizer
                    .sync::<NHCollector>(path)
                    .await
                    .map_err(anyhow::Error::from)
            }
            "exhentai.org" => {
                info!("[registry] sync exhentai for path {}", path);
                self.synchronizer
                    .sync::<EXCollector>(path)
                    .await
                    .map_err(anyhow::Error::from)
            }
            _ => Err(anyhow::anyhow!("no matching collector")),
        }
    }
}
