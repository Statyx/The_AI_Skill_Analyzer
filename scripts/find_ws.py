import json, sys

path = r"c:\Users\cdroinat\AppData\Roaming\Code\User\workspaceStorage\d3962eddf78e5e41b4e9a5c60ce53831\GitHub.copilot-chat\chat-session-resources\1eaa3445-30e8-48d9-8412-8d5ff5b82075\toolu_bdrk_01FWVZBBwHoMBHh6BpRNRTt2__vscode-1774277043551\content.json"
with open(path) as f:
    data = json.load(f)

ws_list = data["results"]["workspaces"]
matches = [w for w in ws_list if any(k in w["displayName"].lower() for k in ["finance", "cdr", "demo"])]

out = []
for w in matches:
    out.append(f"Name: {w['displayName']}")
    out.append(f"  ID: {w['metadata']['workspaceObjectId']}")
    out.append("")

result = "\n".join(out) if out else "No matches found in {} workspaces".format(len(ws_list))
with open("scripts/_ws_results.txt", "w") as f:
    f.write(result)
print(result)
