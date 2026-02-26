import re
from typing import List, Dict
import click

class AwardsParser:
    """
    Парсер спортивных наград.
    """
    PATTERNS = [
        # Пример: фамилия - название награды
        re.compile(r"(?P<name>[А-ЯЁ][а-яё]+(?:\s+[А-ЯЁ][а-яё]+)*)\s*-\s*(?P<award>.+)"),
    ]

    def parse(self, text: str) -> List[Dict]:
        results = []
        for pattern in self.PATTERNS:
            for m in pattern.finditer(text):
                athlete = m.group("name")
                award = m.group("award").strip()
                results.append({"name": athlete, "award": award})
        return results

@click.command()
@click.argument("input_file", type=click.Path(exists=True))
@click.option("--json", is_flag=True, help="Вывести результат в JSON")
def main(input_file, json):
    with open(input_file, encoding="utf-8") as f:
        text = f.read()
    parser = AwardsParser()
    items = parser.parse(text)
    if json:
        import json as _json
        print(_json.dumps(items, ensure_ascii=False, indent=2))
    else:
        for it in items:
            print(f"{it['name']} -> {it['award']}")

if __name__ == "__main__":
    main()
