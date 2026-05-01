#!/usr/bin/env python3

import phonenumbers
import re
from time import sleep
import threading
from typing import Generator
import os
import lxml.etree
from lxml.etree import Element
import sys
import requests
import json
import logging

ns = {"josm": "http://josm.openstreetmap.de/tagging-preset-1.0"}

DEFAULT_LANG = "ka"
LANGS = ["en", "ru", "ka"]


class Issue:
    def __init__(self, message, severity=logging.WARN, item=None, tag=None):
        self.message = message
        self.item = item
        self.tag = tag
        self.severity = severity

    def location(self):
        if self.tag is not None:
            return self.tag.sourceline
        elif self.item is not None:
            return self.item.sourceline
        else:
            return "<unknown>"

    def identifier(self):
        if self.item is not None and self.tag is not None:
            return f"{self.item.attrib["name"]}/{self.tag.attrib["key"]}"
        elif self.item is not None:
            return self.item.attrib["name"]
        else:
            return "<unknown>"


def issues_with_item(item, issues):
    for issue in issues:
        issue.item = item
        yield issue


def issues_with_tag(tag, issues):
    for issue in issues:
        issue.tag = tag
        yield issue


def item_tags(item: Element) -> dict:
    tags = {}
    for key in item.findall("josm:key", ns):
        tags[key.attrib["key"]] = key.attrib["value"]
    return tags


wikidata_cache = {}


def fetch_wikidata_qid(qid: str) -> dict:
    if qid in wikidata_cache:
        return wikidata_cache[qid]
    else:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36"
        }
        url = f"https://www.wikidata.org/w/rest.php/wikibase/v1/entities/items/{qid}"
        logging.debug(f"Fetching {url}")
        resp = requests.get(url, headers=headers)
        while resp.status_code == 429:
            delay = int(resp.headers["retry-after"])
            logging.debug(f"Note: too many wikidata requests, backing off for {delay}")
            sleep(delay)
            resp = requests.get(url, headers=headers)

        obj = json.loads(resp.text)
        wikidata_cache[qid] = obj
        return obj


def match_values(name1, value1, name2, value2) -> Generator[Issue]:
    if value1 is not None and value2 is not None and value1 != value2:
        yield Issue(f"{name1} ({value1}) does not match {name2} ({value2})")


def check_wikidata_labels(wd: dict, item, key: str, lang: str) -> Generator[Issue]:
    for k in filter(lambda x: x.attrib["key"] == key, item.findall("josm:key", ns)):
        name = k.attrib["value"]
        if name != wd.get("labels", {}).get(lang, None) and name not in wd.get(
            "aliases", {}
        ).get(lang, []):
            yield Issue(
                f"{key} ({name}) is neither a label ({wd.get("labels", {}).get(lang, "<no label>")}) nor an alias ({", ".join(wd.get("aliases", {}).get(lang, []))}) of wikidata item {wd["id"]} in language {lang}; if you are sure {key}={wd["id"]} is correct, add a corresponding label or alias to the item",
                tag=item,
            )


def check_wikidata_label_family(item: Element, tag_family: str):
    qid = item_tags(item).get(f"{tag_family}:wikidata", None)
    if qid:
        wd = fetch_wikidata_qid(qid)
        for lang in LANGS:
            key = f"{tag_family}:{lang}"
            yield from check_wikidata_labels(wd, item, key, lang)


contact_keys = [
    "contact:email",
    "contact:facebook",
    "contact:instagram",
    "contact:linkedin",
    "contact:mobile",
    "contact:phone",
    "contact:telegram",
    "contact:tiktok",
    "contact:twitter",
    "contact:website",
    "contact:youtube",
]


def property_for_key(key, value):
    d = {
        "contact:email": "P968",
        "contact:facebook": "P2013",
        "contact:instagram": "P2003",
        "contact:linkedin": "P4264",
        "contact:mobile": "P1329",
        "contact:phone": "P1329",
        "contact:telegram": "P3789",
        "contact:tiktok": "P7085",
        "contact:twitter": "P2002",
        "contact:website": "P856",
        "contact:youtube": "P2397" if "channel" in value else "P11245",
    }
    assert list(d.keys()) == contact_keys
    return d[key]


def last_urlpart(s):
    res = re.match("https://.+/([^/]+)/?", s)
    if res is None:
        raise Exception(f"couldn't match the last URL part")

    return [res.group(1)]


def after_domain(s):
    res = re.match("https://[^/]+/(.*)", s)
    if res is None:
        raise Exception(f"couldn't match the part after FQDN")

    return [res.group(1).rstrip("/")]


def parse_phone(s: str):
    return [
        phonenumbers.format_number(
            phonenumbers.parse(phone), phonenumbers.PhoneNumberFormat.RFC3966
        ).removeprefix("tel:")
        for phone in s.split(";")
    ]

def parse_ytlink(ytlink: str):
    if "@" in ytlink:
        res = re.match("https://.+/@(.*)", ytlink)
        if res is None:
            raise Exception(f"couldn't match the youtube link")
        return [res.group(1)]
    else:
        return last_urlpart(ytlink)

wikidata_value_map = {
    "contact:email": lambda emails: [f"mailto:{email}" for email in emails.split(";")],
    "contact:facebook": last_urlpart,
    "contact:instagram": last_urlpart,
    "contact:linkedin": last_urlpart,
    "contact:mobile": parse_phone,
    "contact:phone": parse_phone,
    "contact:telegram": last_urlpart,
    "contact:tiktok": lambda t: [last_urlpart(t)[0].removeprefix("@")],
    "contact:twitter": last_urlpart,
    "contact:website": lambda t: [t],
    "contact:youtube": parse_ytlink,
}

assert list(wikidata_value_map.keys()) == contact_keys


def check_wikidata_contacts(item: Element):
    tags = item_tags(item)
    keys = item.findall("josm:key", ns)
    wikidatas = [
        fetch_wikidata_qid(tags[tag])
        for tag in ["brand:wikidata", "operator:wikidata"]
        if tag in tags
    ]

    if len(wikidatas) > 0:
        for josm_key in keys:
            key, value = josm_key.attrib["key"], josm_key.attrib["value"]
            if key not in contact_keys:
                continue
            prop = property_for_key(key, value)
            wd_stmts = {
                wd["id"]: [
                    stmt["value"]["content"] for stmt in wd["statements"].get(prop, [])
                ]
                for wd in wikidatas
            }
            wd_values = [value for qid in wd_stmts for value in wd_stmts[qid]]

            try:
                for expected in wikidata_value_map[key](value):
                    if expected not in wd_values:
                        yield Issue(
                            f"contact info {expected} (from {key}={tags[key]}) is not present or does not match the corresponding property ({prop}) any of the associated wikidata items ({str(wd_stmts)})",
                            tag=josm_key,
                        )
            except Exception as e:
                yield Issue(f"failed to parse tag {key}={tags[key]}: {e}", tag=josm_key)



def check_wikidata(item):
    yield from check_wikidata_label_family(item, "brand")
    yield from check_wikidata_label_family(item, "operator")
    yield from check_wikidata_contacts(item)



def check_tag_kartuli(item: Element, tag_family: str):
    tags = item_tags(item)
    l = f"{tag_family}:{DEFAULT_LANG}"
    if tag_family in tags and l in tags:
        yield from issues_with_tag(
            item,
            match_values(f"tag {tag_family}", tags[tag_family], f"tag {l}", tags[l]),
        )


def check_names_kartuli(item):
    yield from check_tag_kartuli(item, "brand")
    yield from check_tag_kartuli(item, "operator")


def check_group(group: Element, diff_map: dict) -> Generator[Issue]:
    for item in group.findall("josm:item", ns):
        name = item.attrib["name"]
        if name not in diff_map or item_tags(item) != diff_map[name]:
            for check in [check_names_kartuli, check_wikidata]:
                yield from issues_with_item(item, check(item))
        else:
            logging.debug(f"Item {name} has not changed, skip")

    for subgroup in group.findall("josm:group", ns):
        yield from check_group(subgroup, diff_map)


def prepare_item_map(group: Element):
    map = {}
    for item in group.findall("josm:item", ns):
        map[item.attrib["name"]] = item_tags(item)

    for subgroup in group.findall("josm:group", ns):
        map = map | prepare_item_map(subgroup)

    return map


def __main__():
    logging.root.setLevel(os.environ.get("LINTER_DEBUG", "INFO"))
    diff_map = {}
    if len(sys.argv) > 2:
        diff_group = (
            lxml.etree.parse(open(sys.argv[2], "r")).getroot().find("josm:group", ns)
        )
        if diff_group is not None:
            diff_map = prepare_item_map(diff_group)

    exit_code = 0

    input_file = sys.argv[1]
    root_group = (
        lxml.etree.parse(open(input_file, "r")).getroot().find("josm:group", ns)
    )
    if root_group is not None:
        for issue in check_group(root_group, diff_map):
            print(
                f"{logging.getLevelName(issue.severity)}:{input_file}#{issue.location()} ({issue.identifier()}): {issue.message}"
            )
            exit_code = 1

    if exit_code == 0:
        print("No issues found")

    exit(exit_code)


__main__()
