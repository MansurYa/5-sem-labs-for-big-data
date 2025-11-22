import subprocess
import sys
from pathlib import Path


def extract_emails_from_repo(repo_path: str, output_file: str):
    """
    Извлекает email адреса авторов из Git репозитория.

    :param repo_path: Путь к Git репозиторию
    :param output_file: Путь к выходному файлу
    """
    repo = Path(repo_path).resolve()

    if not repo.exists() or not (repo / '.git').exists():
        raise ValueError(f"Директория {repo_path} не является Git репозиторием")

    # Получаем список email всех авторов коммитов
    cmd = ['git', '-C', str(repo), 'log', '--format=%ae']

    result = subprocess.run(cmd, capture_output=True, text=True, check=True)

    emails = result.stdout.strip().split('\n')
    emails = [email for email in emails if email]

    # Записываем в файл
    with open(output_file, 'w', encoding='utf-8') as f:
        for email in emails:
            f.write(f'{email}\n')

    print(f"Извлечено {len(emails)} email адресов из репозитория {repo_path}")
    print(f"Данные сохранены в {output_file}")


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Использование: python prepare_data.py <путь_к_репозиторию> <выходной_файл>")
        sys.exit(1)

    repo_path = sys.argv[1]
    output_file = sys.argv[2]

    extract_emails_from_repo(repo_path, output_file)
