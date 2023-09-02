import os
import re
import subprocess

# TODO Extraction
todos = {}
current_branch = subprocess.getoutput('git symbolic-ref --short HEAD').strip()
remote_url = subprocess.getoutput('git config --get remote.origin.url').strip()
username, repo_name = remote_url.split(':')[1].replace('.git', '').split('/')
print(f"{current_branch=}")
print(f"{remote_url=}")
for root, _, files in os.walk("."):
    if root.startswith(("./account_data_fetcher" , "./monitor")):
        print(root)
        if root.startswith("./account_data_fetcher/dependencies/"):
            pass
        for filename in files:
            if filename.endswith(".py"):  # Add other extensions if needed
                with open(os.path.join(root, filename), "r") as f:
                    for i, line in enumerate(f.readlines()):
                        matches = re.findall(r"#TODO\s*: (.+)$", line)
                        print(matches)
                        for match in matches:
                            todos[f"https://github.com/{username}/{repo_name}/blob/{current_branch}/{root[2:]}/{filename}#L{i+1}"] = match

# Markdown Generation
md_content = "## TODOs\n"
for location, todo in todos.items():
    md_content += f"- {todo} ([source]({location}))\n"

current_directory = os.path.dirname(__file__)
base_path = os.path.abspath(os.path.join(current_directory, '..'))

# Update README
readme_path = os.path.join(base_path, "README.md")

# Read existing README
with open(readme_path, "r") as readme:
    existing_content = readme.read().rstrip()

# Remove the last TODO section if exists
if "## TODOs" in existing_content:
    existing_content = existing_content[:existing_content.rfind("## TODOs")]
else:
    new_content = md_content.strip()  # No extra newline if README was empty

# Append the new TODO section
new_content = f"{existing_content}{md_content}".strip()

# Write the updated content back to README
with open(readme_path, "w") as readme:
    readme.write(new_content)