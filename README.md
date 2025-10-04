# 🤖 Knowledge Vault Bot

A Telegram knowledge library and resource management bot for sharing educational content, documents, and media in an organized manner. Built with Python-Telegram-Bot v20.4 with high-performance async architecture.

## 🌟 Features

### 📚 **Content Management**
- **Multi-format Support**: PDF documents, eBooks, photos, videos, and other educational content
- **Collection-Based Organization**: Organize resources by topics or categories
- **Batch Upload**: Upload multiple resources at once with batch system
- **Smart Indexing**: Automatically organize content with intuitive indexing
- **Content Protection**: Toggle protection to prevent unauthorized downloading
- **Custom Welcome Image**: Personalized greeting for new users

### 🔍 **Resource Discovery**
- **Browse Library**: Explore the full collection of knowledge resources
- **Random Pick**: Discover curated content through randomized selection
- **Bookmarks**: Save favorite resources for quick access
- **Top Resources**: View most popular content ranked by user bookmarks

### 🔗 **Sharing System**
- **Direct Links**: Generate shareable links for specific resources or collections
- **Range Links**: Share specific ranges of content (e.g., documents 10-25)
- **Access Control**: Manage who can access specific content
- **Deep Linking**: Direct access via `t.me/YourBot?start=collection_index`

### 🛠️ **Admin Tools**
- **User Management**: Track user interactions and manage permissions
- **Broadcasting**: Send announcements to all users
- **Analytics**: View statistics on user activity and content popularity
- **Auto-Delete System**: Set content to expire after a specified time

## 🚀 **Getting Started**

### Prerequisites
- Python 3.11+
- Telegram Bot Token (from [@BotFather](https://t.me/BotFather))
- Admin Telegram User ID

### Installation

1. **Clone the repository**
```bash
git clone https://github.com/yourusername/knowledge-vault-bot.git
cd knowledge-vault-bot
```

2. **Install dependencies**
```bash
pip install -r requirements.txt
```

3. **Configure environment variables**
Create a `.env` file:
```env
BOT_TOKEN=your_bot_token_here
ADMIN_ID=your_telegram_user_id
```

4. **Update admin ID (Optional)**
The repository includes database files with default settings. You may want to update your admin ID in `admin_list.json`.

5. **Run the bot**
```bash
python bot.py
```

## 📋 **Commands Reference**

### 👤 **User Commands**

| Command | Description |
|---------|-------------|
| `/start` | Start the bot and see welcome message |
| `/help` | Show available commands and features |
| `/random` | Get a random resource from the library |
| `/bookmarks` | View your saved resources |
| `/top` | View most popular resources |

### 🎮 **Admin Commands**

#### Content Management
| Command | Usage | Description |
|---------|-------|-------------|
| `/upload` | Reply to media | Upload resource to database |
| `/remove` | `/remove <tag> <index>` | Remove specific resource |
| `/listvideo` | `/listvideo <tag>` | List resources in a tag |
| `/custom_batch` | `/custom_batch <tag>` | Start batch upload process |
| `/stop_batch` | - | Complete current batch upload |
| `/setwelcomeimage` | `/setwelcomeimage <file_id>` | Set welcome image |

#### Resource Sharing
| Command | Usage | Description |
|---------|-------|-------------|
| `/passlink` | `/passlink <tag>` | Generate shareable link |
| `/revoke` | `/revoke <tag>` | Revoke access to content |
| `/listlinks` | - | List all active links |

#### User Management
| Command | Usage | Description |
|---------|-------|-------------|
| `/broadcast` | `/broadcast <message>` | Send message to all users |
| `/userstats` | - | View user statistics |
| `/add_admin` | `/add_admin <user_id>` | Add a new admin |
| `/list_admins` | - | List all admins |

#### System Management
| Command | Usage | Description |
|---------|-------|-------------|
| `/autodelete` | - | Show auto-delete status |
| `/autodelete on/off` | - | Enable/disable auto-deletion |
| `/protection` | - | Check content protection status |
| `/protectionon` | - | Enable content protection |
| `/protectionoff` | - | Disable content protection |

## 🗄️ **Data Structure**

The bot uses several JSON files for data storage:

- `media_db.json` - Main resource database
- `users_db.json` - User information and statistics
- `favorites_db.json` - User bookmarks
- `active_links.json` - Active shareable links
- `admin_list.json` - List of admin user IDs
- `autodelete_config.json` - Auto-deletion settings

## 🚀 **Deployment Options**

### Railway
1. Push to GitHub repository
2. Deploy on [railway.app](https://railway.app)
3. Set environment variables
4. Connect to your repository for auto-deployment

### Manual Server
```bash
# Clone repository
git clone https://github.com/yourusername/knowledge-vault-bot.git
cd knowledge-vault-bot

# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env with your values

# Run the bot
python bot.py
```

## 📱 **User Interface**

The bot features an intuitive interface with:

- **Inline Keyboards**: Interactive buttons for navigation
- **Bookmarking System**: Easy saving of favorite resources
- **Navigation Controls**: Previous/Next buttons for browsing content
- **Admin Panel**: Comprehensive controls for administrators

## 🔧 **Customization**

- **Welcome Message**: Customize the greeting for new users
- **Global Caption**: Add text that appears with all resources
- **Content Protection**: Toggle download protection
- **Auto-Delete**: Configure automatic deletion timing

## 📈 **Performance**

- **Async Architecture**: Efficient handling of multiple requests
- **Connection Pooling**: Optimized for parallel processing
- **Smart Rate Limiting**: Prevents hitting Telegram API limits
- **Efficient Data Storage**: JSON-based persistence system

---

<div align="center">

**Built for the sharing of knowledge**

</div>