# Managing Personal and Organization GitHub Accounts on the Same Laptop: SSH Keys, Git Config, and Safety Checks

As developers, many of us use a company-issued laptop for our day job while also experimenting with personal projects, open-source contributions, and side POCs on the same machine.

One of the biggest fears that comes with that is accidentally:

- Pushing company code to a personal repository
- Pushing personal code using company credentials
- Committing with the wrong email address
- Mixing SSH keys between personal and organizational GitHub accounts

In this post, I'll walk through a setup that lets you safely juggle both personal and organization repositories on one laptop — with Git automatically doing the right thing, instead of you having to remember to switch anything by hand.

## Why I Built This Setup

I frequently work with both organization repositories and personal projects on the same laptop.

Initially, I had to remember which GitHub account I was using, manually verify Git identities, and double-check repository remotes before pushing changes.

That worked until the day I almost pushed using the wrong identity.

Rather than relying on memory, I wanted a setup where Git automatically selected the correct account, SSH key, and commit identity based on the repository location.

This article documents the approach I now use daily.

## Who is this for?

This setup is useful if:

- You use a company laptop for work and personal projects
- You contribute to open source
- You maintain multiple GitHub accounts
- You want to avoid accidental pushes using the wrong identity

## The Goal

Here's what the final setup gives you:

- Organization repositories automatically use your work GitHub account
- Personal repositories automatically use your personal GitHub account
- Git identity (name/email) switches automatically based on folder location
- Separate SSH keys per account, so authentication never crosses over
- A safety net that blocks a push if something still looks mismatched

## Architecture Overview

```text
                     ┌─────────────────┐
                     │ GitHub Work     │
                     └────────┬────────┘
                              │
                    id_ed25519_work
                              │
                              ▼
                    ~/work/equinix/*

                     ┌─────────────────┐
                     │ GitHub Personal │
                     └────────┬────────┘
                              │
                 id_ed25519_personal
                              │
                              ▼
                      ~/personal/*
```

The folder location determines the Git identity.

The SSH host alias determines the SSH key.

Together they ensure the correct account is used automatically.

## Prerequisites

Before starting, ensure:

- Git is installed
- SSH is available
- You have access to both GitHub accounts
- You can add SSH keys to both accounts

Verify:

```bash
git --version
ssh -V
```

## Step 1: Separate Your Repositories by Folder

Everything in this setup hinges on one simple decision: **where you physically keep your repos on disk.**

```
~/work/equinix/
├── project-a
├── project-b
└── project-c

~/personal/
├── java-sonarqube-maven-jacoco-integration
├── git-action-basic-exercise
└── side-projects
```

Create them now:

```bash
mkdir -p ~/work/equinix
mkdir -p ~/personal
```

Every other piece of this setup — Git identity, SSH key selection — is triggered off this folder structure. So the one rule to never break is: **work repos always go under `~/work/equinix/`, personal repos always go under `~/personal/`.**

## Step 2: Generate Separate SSH Keys

SSH keys are what let you authenticate with GitHub at all — without one, you can't even clone a private repo, let alone push to it. So this comes before anything else functional.

Generate a key for your work account:

```bash
ssh-keygen -t ed25519 -f ~/.ssh/id_ed25519_work -C "your-work-email@company.com"
```

Generate a separate key for your personal account:

```bash
ssh-keygen -t ed25519 -f ~/.ssh/id_ed25519_personal -C "your-personal-email@gmail.com"
```

Each command produces two files — a private key (never share it) and a `.pub` file (safe to share). Upload the public key to the matching GitHub account:

```bash
pbcopy < ~/.ssh/id_ed25519_work.pub
```
→ Work GitHub account → **Settings → SSH and GPG keys → New SSH key**

```bash
pbcopy < ~/.ssh/id_ed25519_personal.pub
```
→ Personal GitHub account → **Settings → SSH and GPG keys → New SSH key**

## Step 3: Tell SSH Which Key to Use, Per Account

GitHub only recognizes one real hostname: `github.com`. But you now have two keys, and SSH needs a way to know which one to use for which account. The trick is creating a **fake alias hostname** that secretly points back to the real one.

Edit `~/.ssh/config`:

```
# Work Account
Host github.com
    HostName github.com
    User git
    IdentityFile ~/.ssh/id_ed25519_work
    IdentitiesOnly yes

# Personal Account
Host github-personal
    HostName github.com
    User git
    IdentityFile ~/.ssh/id_ed25519_personal
    IdentitiesOnly yes
```

- `github.com` is used as-is for work repos → uses the work key.
- `github-personal` is a made-up alias. SSH sees this name, silently connects to the *real* `github.com`, but authenticates with the personal key instead.
- `IdentitiesOnly yes` stops SSH from trying every key in `~/.ssh/` and getting confused — it only tries the one you specified.

Lock down permissions (SSH refuses to use keys with overly open permissions):

```bash
chmod 700 ~/.ssh
chmod 600 ~/.ssh/id_ed25519_work ~/.ssh/id_ed25519_personal
chmod 644 ~/.ssh/id_ed25519_work.pub ~/.ssh/id_ed25519_personal.pub
chmod 600 ~/.ssh/config
```

Test both connections:

```bash
ssh -T github.com           # should authenticate as your work account
ssh -T github-personal      # should authenticate as your personal account
```

Expected response for each:

Example output:

```bash
$ ssh -T github.com
```

```text
Hi work-user! You've successfully authenticated, but GitHub does not provide shell access.
```

```bash
$ ssh -T github-personal
```

```text
Hi sumithazra! You've successfully authenticated, but GitHub does not provide shell access.
```

If the wrong username shows up, double check the `IdentityFile` paths in `~/.ssh/config`.

## Step 4: Auto-Switch Git Identity by Folder

Now that authentication works, let's make sure your **commit author info** (name/email) also switches automatically depending on where the repo lives — so you never accidentally commit work code under your personal email, or vice versa.

Create your global `~/.gitconfig`:

```ini
[user]
    name = Your Name
    email = your-work-email@company.com

[includeIf "gitdir:~/work/equinix/"]
    path = ~/.gitconfig-work

[includeIf "gitdir:~/personal/"]
    path = ~/.gitconfig-personal
```

`includeIf "gitdir:..."` tells Git: *"if the current repo's path is inside this folder, also load this extra config file."* The top `[user]` block is just a fallback for repos outside both folders.

Create `~/.gitconfig-work`:

```ini
[user]
    name = Your Name
    email = your-work-email@company.com
```

Create `~/.gitconfig-personal`:

```ini
[user]
    name = Your Name
    email = your-personal-email@gmail.com
```

From now on, just `cd`-ing into a repo under the right folder is enough — Git silently picks the correct identity. No manual `git config` per repo, ever again.

## Step 5: Add GPG Signing for Work Commits (Optional but Common)

Many companies require commits to be **signed**, so GitHub shows a "Verified" badge proving a commit really came from you and wasn't tampered with or spoofed. This is a separate system from SSH — SSH controls *whether you can push*; GPG controls *whether the commit is provably yours*. Order between the two doesn't matter functionally, but it's natural to do this after SSH is already working, since testing a signed commit requires being able to push in the first place.

Install GPG if needed:

```bash
brew install gnupg
```

Generate a key:

```bash
gpg --full-generate-key
```

Use your **work email** when prompted, so it matches `your-work-email@company.com` in `.gitconfig-work`.

Find your key ID:

```bash
gpg --list-secret-keys --keyid-format=long
```

```
sec   rsa4096/3AA5C34371567BD2 2026-06-20 [SC]
uid                 Your Name <your-work-email@company.com>
```

Export and upload the public key:

```bash
gpg --armor --export 3AA5C34371567BD2
```

Paste the output into your **work** GitHub account → **Settings → SSH and GPG keys → New GPG key**.

Update `~/.gitconfig-work` to enable signing:

```ini
[user]
    name = Your Name
    email = your-work-email@company.com
    signingkey = 3AA5C34371567BD2

[commit]
    gpgsign = true

[gpg]
    program = gpg
```

Test it in a work repo:

```bash
cd ~/work/equinix/some-repo
git commit -S -m "test signed commit"
git push
```

Check the commit on GitHub — it should show a green **Verified** badge.

> **Worth knowing:** SSH keys and GPG keys solve two unrelated problems, and neither one knows the other exists. SSH only checks *"does this key let you push to this account?"* GPG only checks *"was this commit cryptographically signed by you?"* And separately, GitHub does one more cosmetic check after the fact: it looks at the **commit author email** and tries to match it to a verified email on *some* GitHub account, purely to decide which avatar to show next to the commit. None of these three checks talk to each other — which is exactly why it's possible to push successfully with the right SSH key but have the commit attributed to the wrong identity if your `.gitconfig` email doesn't match. Keeping the folder structure consistent is what prevents that.

## Step 6: Clone Repositories Using the Correct Remote

This is the step that ties folder, identity, and key together. The clone URL you use determines which SSH key gets used (because of the alias from Step 3). The folder you clone into determines which Git identity gets used (because of the `includeIf` from Step 4).

**Work repository:**

```bash
cd ~/work/equinix
git clone git@github.com:equinix-enterpriseapps/project.git
```

**Personal repository — note the `github-personal` alias instead of `github.com`:**

```bash
cd ~/personal
git clone git@github-personal:sumithazra/git_action_basic_exercise.git
```

### Verify it worked

```bash
git config user.email
git remote -v
```

Expected for a personal repo:

```
origin  git@github-personal:username/repository.git (fetch)
```

Expected for a work repo:

```
origin  git@github.com:organization/repository.git (fetch)
```

If either of these looks wrong, fix the remote before pushing anything:

```bash
git remote set-url origin git@github-personal:username/repository.git
```

## Step 7: A Pre-Push Hook as a Final Safety Net

Even with all of the above in place, mistakes can still slip through — for example, if you manually typed the wrong clone URL once. A pre-push hook runs automatically before every `git push` and can simply refuse the push if something doesn't add up.

```bash
mkdir -p ~/.githooks
git config --global core.hooksPath ~/.githooks
```

Create `~/.githooks/pre-push`:

```bash
#!/bin/bash

remote_url=$(git remote get-url origin)
current_email=$(git config user.email)
repo_path=$(pwd)

if [[ "$repo_path" == *"/work/equinix/"* ]]; then
    if [[ "$current_email" != "your-work-email@company.com" ]]; then
        echo "❌ Wrong email for a work repo. Push blocked."
        exit 1
    fi
fi

if [[ "$repo_path" == *"/personal/"* ]]; then
    if [[ "$current_email" != "your-personal-email@gmail.com" ]]; then
        echo "❌ Wrong email for a personal repo. Push blocked."
        exit 1
    fi
    if [[ "$remote_url" != *"github-personal"* ]]; then
        echo "❌ Personal repo isn't using the github-personal SSH alias. Push blocked."
        exit 1
    fi
fi

exit 0
```

Make it executable:

```bash
chmod +x ~/.githooks/pre-push
```

Now every push, in every repo on your machine, is checked against the folder, the email, and the remote URL before it's allowed through.

## Quick Reference: Everyday Commands

| Action | Command |
|---|---|
| Clone (work) | `git clone git@github.com:org/repo.git` |
| Clone (personal) | `git clone git@github-personal:user/repo.git` |
| Check status | `git status` |
| Stage changes | `git add .` |
| Commit | `git commit -m "message"` |
| Commit (signed) | `git commit -S -m "message"` |
| First push | `git push --set-upstream origin main` |
| Push | `git push` |
| Pull | `git pull origin main` |
| New branch | `git checkout -b feature/my-feature` |
| Switch branch | `git checkout main` |
| Merge | `git merge feature/my-feature` |
| View history | `git log --oneline` |
| List branches | `git branch -a` |


## Troubleshooting

### SSH Uses the Wrong Account

```bash
ssh -vT github-personal
```

Look for:

```text
Offering public key:
```

and verify the expected key path.

### Wrong Commit Email

```bash
git config user.email
```

Verify the repository is under the correct folder structure.

### Verify Which Config File Git Loaded

```bash
git config --show-origin user.email
```

Example:

```text
file:/Users/sumit/.gitconfig-personal
```

### Verify Remote URL

```bash
git remote -v
```

Personal repositories should use:

```text
git@github-personal:user/repo.git
```

Work repositories should use:

```text
git@github.com:org/repo.git
```

## Summary

| Layer | Mechanism | Protects Against |
|---|---|---|
| Folder structure | `~/work/...` vs `~/personal/...` | Mixing repos physically |
| SSH keys + alias | `github.com` vs `github-personal` | Wrong account authenticating |
| `.gitconfig` + `includeIf` | Auto-loaded identity files | Wrong name/email on commits |
| GPG signing | Per-identity signing key | Unverified/spoofed commits |
| Pre-push hook | Script run before every push | Anything that still slips through |

By combining folder separation, dedicated SSH keys, automatic Git identity switching, optional GPG signing, and a pre-push safety hook, you can remove nearly all of the manual effort involved in managing multiple GitHub accounts.

Instead of relying on memory and double-checking every push, Git automatically enforces the correct identity and authentication flow based on where the repository lives.

Set it up once, and it quietly protects you every day.


---

## About the Author

I'm Sumit Hazra, a backend engineer focused on distributed systems, cloud-native architectures, Java/Spring Boot applications, Kafka, PostgreSQL, and platform engineering.

I enjoy sharing practical engineering solutions learned from real-world projects.
