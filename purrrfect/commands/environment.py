import os

from purrrfect.commands import CONFIG, PurrrfectConfigModel


def generate_url(url_text: str, url: str, short_link: str = "🔗"):
    if CONFIG.short_link is True or (
        CONFIG.short_link is None and os.getenv("TERM_PROGRAM") == "iTerm.app"
    ):
        return f"[link={url}]{short_link}[/link]"
    else:
        return url


def get_url_length(config: PurrrfectConfigModel):
    if config.short_link is None:
        return
