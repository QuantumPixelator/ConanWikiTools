import sys
import os
import re
import sqlite3
import requests
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QTabWidget, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QLabel, QTextEdit, QProgressBar, QFileDialog, QComboBox,
    QListWidget, QFrame, QGridLayout, QLineEdit, QScrollArea, QMessageBox
)
from PySide6.QtCore import Qt, QThread, Signal, QWaitCondition, QMutex
from PySide6.QtGui import QFont, QColor

# Conan-themed stylesheet
CONAN_STYLE = """
    QWidget {
        background-color: #1F1B16;
        color: #E0C097;
        font-family: Arial, sans-serif;
    }
    QLabel {
        font-size: 14px;
    }
    QPushButton {
        background-color: #6F4E37;
        color: #E0C097;
        border: 1px solid #8B5A2B;
        padding: 8px;
        border-radius: 4px;
    }
    QPushButton:hover {
        background-color: #8B5A2B;
    }
    QTextEdit, QListWidget {
        background-color: #3B3024;
        color: #E0C097;
        border: 1px solid #6F4E37;
    }
    QProgressBar {
        border: 1px solid #6F4E37;
        background-color: #3B3024;
        color: #E0C097;
    }
    QProgressBar::chunk {
        background-color: #C19A6B;
    }
    QComboBox, QLineEdit {
        background-color: #3B3024;
        color: #E0C097;
        border: 1px solid #6F4E37;
        padding: 5px;
    }
"""

# Scraper Tab
class ScraperTab(QWidget):
    def __init__(self):
        super().__init__()
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout()

        self.label = QLabel("Conan Exiles Wiki Scraper")
        self.label.setAlignment(Qt.AlignCenter)
        self.label.setFont(QFont("Arial", 18, QFont.Bold))

        self.status_text = QTextEdit()
        self.status_text.setReadOnly(True)

        self.progress_bar = QProgressBar()
        self.progress_bar.setValue(0)

        self.scrape_button = QPushButton("Start Scraping")
        self.scrape_button.clicked.connect(self.toggle_scraping)

        layout.addWidget(self.label)
        layout.addWidget(self.status_text)
        layout.addWidget(self.progress_bar)
        layout.addWidget(self.scrape_button)

        self.setLayout(layout)

        self.worker = None
        self.is_scraping = False
        self.is_paused = False

    def toggle_scraping(self):
        if not self.is_scraping:
            save_dir = QFileDialog.getExistingDirectory(self, "Select save folder")
            if save_dir:
                self.worker = ScrapeWorker(save_dir)
                self.worker.progress.connect(self.update_progress)
                self.worker.status_update.connect(self.update_status)
                self.worker.scraping_complete.connect(self.scraping_finished)
                self.worker.start()
                self.is_scraping = True
                self.is_paused = False
                self.scrape_button.setText("Stop Scraping")
                self.scrape_button.setStyleSheet("background-color: red; color: white;")
            else:
                self.status_text.append("Scraping canceled. No folder selected.")
        elif self.is_scraping and not self.is_paused:
            self.worker.pause()
            self.is_paused = True
            self.scrape_button.setText("Resume Scraping")
            self.scrape_button.setStyleSheet("background-color: yellow; color: black;")
            self.status_text.append("Scraping paused.")
        elif self.is_paused:
            self.worker.resume()
            self.is_paused = False
            self.scrape_button.setText("Stop Scraping")
            self.scrape_button.setStyleSheet("background-color: red; color: white;")
            self.status_text.append("Scraping resumed.")

    def update_progress(self, value):
        self.progress_bar.setValue(value)

    def update_status(self, message):
        self.status_text.append(message)

    def scraping_finished(self):
        self.status_text.append("Scraping process is complete!")
        self.progress_bar.setValue(100)
        self.is_scraping = False
        self.is_paused = False
        self.scrape_button.setText("Start Scraping")
        self.scrape_button.setStyleSheet("")

class ScrapeWorker(QThread):
    progress = Signal(int)
    status_update = Signal(str)
    scraping_complete = Signal()

    def __init__(self, save_dir):
        super().__init__()
        self.save_dir = save_dir
        self.total_pages = 0
        self.pages_scraped = 0
        self.is_paused = False
        self.pause_condition = QWaitCondition()
        self.mutex = QMutex()

    def pause(self):
        self.mutex.lock()
        self.is_paused = True
        self.mutex.unlock()

    def resume(self):
        self.mutex.lock()
        self.is_paused = False
        self.pause_condition.wakeAll()
        self.mutex.unlock()

    def sanitize_filename(self, title):
        return re.sub(r'[<>:"/\\|?*]', '_', title)

    def fetch_all_pages(self):
        all_pages = set()
        categories = ["Thralls", "NPCs", "Creatures", "Pets"]
        for category in categories:
            params = {
                "action": "query",
                "format": "json",
                "list": "categorymembers",
                "cmtitle": f"Category:{category}",
                "cmlimit": "max"
            }
            while True:
                response = requests.get("https://conanexiles.fandom.com/api.php", params=params)
                if response.status_code == 200:
                    data = response.json()
                    pages = data['query']['categorymembers']
                    for page in pages:
                        all_pages.add(page['title'])
                    if 'continue' in data:
                        params['cmcontinue'] = data['continue']['cmcontinue']
                    else:
                        break
                else:
                    break
        return list(all_pages)

    def fetch_page_content(self, page_title):
        params = {
            "action": "query",
            "format": "json",
            "prop": "revisions",
            "titles": page_title,
            "rvprop": "content"
        }
        response = requests.get("https://conanexiles.fandom.com/api.php", params=params)
        if response.status_code == 200:
            data = response.json()
            pages = data['query']['pages']
            for page_id, page_info in pages.items():
                return page_info['revisions'][0]['*'] if 'revisions' in page_info else None
        return None

    def save_page(self, content, page_title):
        page_title_cleaned = self.sanitize_filename(page_title)
        file_path = os.path.join(self.save_dir, f"{page_title_cleaned}.txt")
        with open(file_path, 'w', encoding="utf-8") as file:
            file.write(content)

    def run(self):
        self.status_update.emit("Fetching thrall, NPC, and creature pages from the wiki...")
        pages = self.fetch_all_pages()
        if pages:
            self.total_pages = len(pages)
            self.status_update.emit(f"Found {self.total_pages} pages. Starting to download content...")
            for i, page_title in enumerate(pages):
                self.mutex.lock()
                while self.is_paused:
                    self.pause_condition.wait(self.mutex)
                self.mutex.unlock()
                page_title_cleaned = self.sanitize_filename(page_title)
                file_path = os.path.join(self.save_dir, f"{page_title_cleaned}.txt")
                if os.path.exists(file_path):
                    self.status_update.emit(f"Skipping already downloaded: {page_title}")
                    self.pages_scraped += 1
                    continue
                self.status_update.emit(f"Downloading: {page_title}")
                content = self.fetch_page_content(page_title)
                if content:
                    self.save_page(content, page_title)
                else:
                    self.status_update.emit(f"Failed to fetch content for {page_title}")
                self.pages_scraped += 1
                self.progress.emit(int((self.pages_scraped / self.total_pages) * 100))
            self.status_update.emit("Scraping completed!")
            self.scraping_complete.emit()
        else:
            self.status_update.emit("Failed to retrieve pages from the wiki.")

class PopulateWorker(QThread):
    progress = Signal(int)
    status_update = Signal(str)
    finished = Signal()

    def __init__(self, files):
        super().__init__()
        self.files = files

    def run(self):
        for i, file_path in enumerate(self.files):
            file_name = os.path.basename(file_path)
            try:
                thrall_data = self.parse_thrall_file(file_path)
                result = self.insert_or_update_data(thrall_data)
                if "INVALID CLASS" in result:
                    self.status_update.emit(f"<b style='color:red;'>{file_name}: {result}</b>")
                else:
                    self.status_update.emit(f"<b>{file_name}: {result}</b>")
            except Exception as e:
                self.status_update.emit(f"<b style='color:red;'>{file_name}: ERROR - {e}</b>")
            self.progress.emit(int((i + 1) / len(self.files) * 100))
        self.finished.emit()

    def parse_thrall_file(self, file_path):
        data = {}
        with open(file_path, "r", encoding='utf-8') as file:
            for line in file:
                if "=" in line:
                    key, value = line.strip().split("=", 1)
                    data[key.strip().lower()] = value.strip() or "N/A"
        return data

    def insert_or_update_data(self, data):
        conn = sqlite3.connect("thralls.db")
        cursor = conn.cursor()
        thrall_class = data.get("class", "").lower()
        if thrall_class not in [
            "alchemist", "archer", "armorer", "bearer", "blacksmith",
            "carpenter", "cook", "fighter", "performer", "priest",
            "smelter", "sorcerer", "tanner", "taskmaster", "pet", "animal"
        ]:
            conn.close()
            return f"INVALID CLASS: {thrall_class.upper()}"
        table_name = thrall_class.title() if thrall_class not in ["pet", "animal"] else ("Pets" if thrall_class == "pet" else "Animals")
        cursor.execute(f"""
            INSERT INTO {table_name} (
                name, id, class, health, strength, agility, vitality, grit,
                bonus_vitality, level_rate, armor, incoming_damage_reduction,
                killed_xp, temperament, gender, thrallable, race, faction,
                description, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET
                id=excluded.id,
                health=excluded.health,
                strength=excluded.strength,
                agility=excluded.agility,
                vitality=excluded.vitality,
                grit=excluded.grit,
                bonus_vitality=excluded.bonus_vitality,
                level_rate=excluded.level_rate,
                armor=excluded.armor,
                incoming_damage_reduction=excluded.incoming_damage_reduction,
                killed_xp=excluded.killed_xp,
                temperament=excluded.temperament,
                gender=excluded.gender,
                thrallable=excluded.thrallable,
                race=excluded.race,
                faction=excluded.faction,
                description=excluded.description,
                notes=excluded.notes
        """, (
            data.get("name", "N/A"), data.get("id", "N/A"), data.get("class", "N/A"),
            data.get("health", "N/A"), data.get("strength", "N/A"),
            data.get("agility", "N/A"), data.get("vitality", "N/A"),
            data.get("grit", "N/A"), data.get("bonus vitality", "N/A"),
            data.get("level rate", "N/A"), data.get("armor", "N/A"),
            data.get("incoming damage reduction", "N/A"), data.get("killed xp", "N/A"),
            data.get("temperament", "N/A"), data.get("gender", "N/A"),
            data.get("thrallable", "N/A"), data.get("race", "N/A"),
            data.get("faction", "N/A"), data.get("description", "N/A"),
            data.get("notes", "N/A")
        ))
        conn.commit()
        conn.close()
        return "SUCCESS"
class FormatterTab(QWidget):
    def __init__(self):
        super().__init__()
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout()

        self.label = QLabel("Wiki Data Formatter")
        self.label.setAlignment(Qt.AlignCenter)
        self.label.setFont(QFont("Arial", 18, QFont.Bold))

        button_layout = QHBoxLayout()
        self.select_files_btn = QPushButton("Select Input Files")
        self.select_files_btn.clicked.connect(self.select_input_files)
        self.select_folder_btn = QPushButton("Select Output Folder")
        self.select_folder_btn.clicked.connect(self.select_output_folder)
        self.start_btn = QPushButton("Start Processing")
        self.start_btn.clicked.connect(self.start_processing)
        self.stop_btn = QPushButton("Stop Processing")
        self.stop_btn.clicked.connect(self.stop_processing)
        button_layout.addWidget(self.select_files_btn)
        button_layout.addWidget(self.select_folder_btn)
        button_layout.addWidget(self.start_btn)
        button_layout.addWidget(self.stop_btn)

        self.progress_bar = QProgressBar()

        self.log_box = QTextEdit()
        self.log_box.setReadOnly(True)

        layout.addWidget(self.label)
        layout.addLayout(button_layout)
        layout.addWidget(self.progress_bar)
        layout.addWidget(self.log_box)

        self.setLayout(layout)

        self.input_files = []
        self.output_folder = ""

    def select_input_files(self):
        files, _ = QFileDialog.getOpenFileNames(self, "Select Input Files", "", "Text Files (*.txt)")
        if files:
            self.input_files = files
            self.log_message(f"Selected {len(files)} input files.", "black")

    def select_output_folder(self):
        folder = QFileDialog.getExistingDirectory(self, "Select Output Folder")
        if folder:
            self.output_folder = folder
            self.log_message("Output path selected.", "black")

    def start_processing(self):
        if not self.input_files or not self.output_folder:
            QMessageBox.warning(self, "Error", "Please select input files and output folder.")
            return
        self.thread = FileProcessorThread(self.input_files, self.output_folder)
        self.thread.progress.connect(self.update_progress)
        self.thread.log.connect(self.log_message)
        self.thread.start()
        self.log_message("Started processing files.", "black")

    def stop_processing(self):
        if hasattr(self, 'thread') and self.thread.isRunning():
            self.thread.stop()

    def update_progress(self, value):
        self.progress_bar.setValue(value)

    def log_message(self, message, color):
        color_map = {"black": QColor("white"), "darkgreen": QColor("green"), "darkred": QColor("red")}
        self.log_box.setTextColor(color_map.get(color, QColor("white")))
        self.log_box.append(message)
        self.log_box.setTextColor(QColor("white"))

class FileProcessorThread(QThread):
    progress = Signal(int)
    log = Signal(str, str)

    def __init__(self, input_files, output_folder):
        super().__init__()
        self.input_files = input_files
        self.output_folder = output_folder
        self.running = True
        self.success_count = 0
        self.failed_count = 0

    def run(self):
        total_files = len(self.input_files)
        self.log.emit(f"Total files selected: {total_files}", "black")
        for idx, file_path in enumerate(self.input_files):
            if not self.running:
                self.log.emit("Processing stopped by user.", "black")
                break
            try:
                file_name = os.path.basename(file_path)
                self.log.emit(f"Processing: {file_name}", "black")
                with open(file_path, 'r', encoding='utf-8') as file:
                    content = file.read()
                processed_data, name = self.process_file(content)
                output_file = os.path.join(self.output_folder, f"{name}.txt")
                with open(output_file, 'w', encoding='utf-8') as file:
                    file.write(processed_data)
                self.success_count += 1
                self.log.emit(f"Success: {name}.txt", "darkgreen")
            except Exception as e:
                self.failed_count += 1
                self.log.emit(f"Failed: {file_name}. Error: {str(e)}", "darkred")
            self.progress.emit(int(((idx + 1) / total_files) * 100))
        self.log.emit(f"Processing complete. Total: {total_files}, Success: {self.success_count}, Failed: {self.failed_count}", "black")

    def process_file(self, content):
        fields = {
            'name': r'\| name = (.*?)\n',
            'id': r'\| id = (.*?)\n',
            'class': r'\| class = (.*?)\n',
            'health': r'\| Health = (.*?)\n',
            'strength': r'\| Strength = (.*?)\n',
            'agility': r'\| Agility = (.*?)\n',
            'vitality': r'\| Vitality = (.*?)\n',
            'grit': r'\| Grit = (.*?)\n',
            'bonus_vitality': r'\| BonusVit = (.*?)\n',
            'armor': r'\| NPCArmor = (.*?)\n',
            'damage_reduction': r'\| NPCDRArmor = (.*?)\n',
            'xp': r'\| NPCKillXP = (.*?)\n',
            'temperament': r'\| NPCTemperament = (.*?)\n',
            'gender': r'\| gender = (.*?)\n',
            'thrallable': r'\| thrallable = (.*?)\n',
            'race': r'\| race = (.*?)\n',
            'faction': r'\| fac = (.*?)\n',
            'level_rate': r'\| levelCurve = (.*?)\n',
            'notes': r'==Notes==\n.*?\n(.*?)\n'
        }
        data = {}
        for key, pattern in fields.items():
            match = re.search(pattern, content, re.DOTALL)
            value = match.group(1).strip() if match else ('N/A' if key not in ['health', 'xp'] else '0')
            data[key] = value
        if 'Any particular data' in data['notes'] or data['notes'].startswith('<!--'):
            data['notes'] = 'N/A'
        notes_cleaned = re.sub(r'\[\[(.*?)\]\]', lambda m: m.group(1).split('|')[-1], data['notes'])
        notes_cleaned = re.sub(r'\{\{ItemLink\|(.*?)\}\}', lambda m: m.group(1), notes_cleaned)
        notes_cleaned = re.sub(r'<[^>]+>', '', notes_cleaned)
        description = f"{data['name']} is a named, Tier 4 {data['class']} NPC of the {data['faction']} faction." if data['faction'] != 'N/A' else "No faction assigned."
        notes = notes_cleaned.replace("{{PAGENAME}}", data['name']).replace("'''", "")
        output = [
            f"Name = {data['name']}",
            f"ID = {data['id']}",
            f"Class = {data['class']}",
            f"Health = {data['health']}",
            f"Strength = {data['strength'] if data['strength'] != '' else '0'}",
            f"Agility = {data['agility'] if data['agility'] != '' else '0'}",
            f"Vitality = {data['vitality'] if data['vitality'] != '' else '0'}",
            f"Grit = {data['grit']}",
            f"Bonus Vitality = {data['bonus_vitality']}",
            f"Level Rate = {data['level_rate']}",
            f"Armor = {data['armor']}",
            f"Incoming Damage Reduction = {data['damage_reduction']}",
            f"Killed XP = {data['xp']}",
            f"Temperament = {data['temperament']}",
            f"Gender = {data['gender']}",
            f"Thrallable = {data['thrallable']}",
            f"Race = {data['race']}",
            f"Faction = {data['faction']}",
            f"Description = {description}",
            f"Notes = {notes}"
        ]
        return '\n'.join(output), data['name']

    def stop(self):
        self.running = False

# DB Viewer Tab
class DBViewerTab(QWidget):
    def __init__(self):
        super().__init__()
        self.init_ui()

    def init_ui(self):
        self.main_layout = QVBoxLayout()

        self.header_layout = QVBoxLayout()
        self.header_label = QLabel("<b>Choose Category:</b>")
        self.header_label.setAlignment(Qt.AlignLeft)
        self.table_dropdown = QComboBox()
        self.table_dropdown.addItems([
            "Alchemist", "Archer", "Armorer", "Bearer", "Blacksmith",
            "Carpenter", "Cook", "Fighter", "Performer", "Priest",
            "Smelter", "Sorcerer", "Tanner", "Taskmaster", "Pets", "Animals"
        ])
        self.table_dropdown.currentTextChanged.connect(self.populate_names)

        self.search_layout = QHBoxLayout()
        self.search_field = QLineEdit()
        self.search_field.setPlaceholderText("Enter search conditions (e.g., Gender=female AND Level Rate=fast)")
        self.search_button = QPushButton("Search")
        self.search_button.clicked.connect(self.perform_search)
        self.search_layout.addWidget(self.search_field)
        self.search_layout.addWidget(self.search_button)

        self.header_layout.addWidget(self.header_label)
        self.header_layout.addWidget(self.table_dropdown)
        self.header_layout.addLayout(self.search_layout)

        self.separator = QFrame()
        self.separator.setFrameShape(QFrame.HLine)

        self.lower_layout = QHBoxLayout()
        self.name_list = QListWidget()
        self.name_list.setFixedWidth(250)
        self.name_list.itemClicked.connect(self.display_data)

        self.details_scroll = QScrollArea()
        self.details_scroll.setWidgetResizable(True)
        self.details_widget = QWidget()
        self.details_layout = QGridLayout()
        self.details_widget.setLayout(self.details_layout)
        self.details_scroll.setWidget(self.details_widget)

        self.fields = [
            "Name", "ID", "Class", "Health", "Strength", "Agility",
            "Vitality", "Grit", "Bonus Vitality", "Level Rate", "Armor",
            "Incoming Damage Reduction", "Killed XP", "Temperament",
            "Gender", "Thrallable", "Race", "Faction", "Description", "Notes"
        ]
        self.detail_labels = {}
        for i, field in enumerate(self.fields):
            label = QLabel(f"<b>{field}:</b>")
            value = QLabel("N/A")
            value.setWordWrap(True)
            self.details_layout.addWidget(label, i, 0)
            self.details_layout.addWidget(value, i, 1)
            self.detail_labels[field] = value

        self.lower_layout.addWidget(self.name_list)
        self.lower_layout.addWidget(self.details_scroll)

        self.main_layout.addLayout(self.header_layout)
        self.main_layout.addWidget(self.separator)
        self.main_layout.addLayout(self.lower_layout)
        self.setLayout(self.main_layout)

        self.conn = sqlite3.connect("thralls.db")
        self.cursor = self.conn.cursor()
        self.populate_names()

    def populate_names(self):
        self.name_list.clear()
        table = self.table_dropdown.currentText()
        try:
            self.cursor.execute(f"SELECT name FROM {table} ORDER BY name ASC")
            names = self.cursor.fetchall()
            for name in names:
                self.name_list.addItem(name[0])
        except Exception as e:
            self.name_list.addItem(f"Error loading names: {e}")

    def display_data(self, item):
        table = self.table_dropdown.currentText()
        name = item.text()
        try:
            self.cursor.execute(f"SELECT * FROM {table} WHERE name = ?", (name,))
            data = self.cursor.fetchone()
            if data:
                for i, field in enumerate(self.fields):
                    self.detail_labels[field].setText(data[i] if data[i] else "N/A")
        except Exception as e:
            for field in self.fields:
                self.detail_labels[field].setText("Error")

    def perform_search(self):
        query = self.search_field.text().strip()
        table = self.table_dropdown.currentText()
        self.name_list.clear()
        if not query:
            self.populate_names()
            return
        try:
            conditions = []
            params = []
            for condition in query.split(" AND "):
                operators = [">=", "<=", ">", "<", "="]
                for operator in operators:
                    if operator in condition:
                        field, value = condition.split(operator, 1)
                        field = field.strip().lower().replace(" ", "_")
                        value = value.strip()
                        conditions.append(f"[{field}] {operator} ?")
                        params.append(value)
                        break
            sql = f"SELECT name FROM {table}"
            if conditions:
                sql += " WHERE " + " AND ".join(conditions)
            self.cursor.execute(sql, params)
            results = self.cursor.fetchall()
            if results:
                for result in results:
                    self.name_list.addItem(result[0])
            else:
                self.name_list.addItem("No results found.")
        except Exception as e:
            self.name_list.addItem(f"Search error: {e}")

# DB Populator Tab
class DBPopulatorTab(QWidget):
    def __init__(self):
        super().__init__()
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout()

        self.label = QLabel("Database Populator")
        self.label.setAlignment(Qt.AlignCenter)
        self.label.setFont(QFont("Arial", 18, QFont.Bold))

        self.load_button = QPushButton("Load Thrall Files")
        self.load_button.clicked.connect(self.load_files)

        self.progress = QProgressBar()

        self.log = QTextEdit()
        self.log.setReadOnly(True)

        layout.addWidget(self.label)
        layout.addWidget(self.load_button)
        layout.addWidget(self.progress)
        layout.addWidget(self.log)

        self.setLayout(layout)

    def load_files(self):
        files, _ = QFileDialog.getOpenFileNames(self, "Select Thrall Files", "", "Text Files (*.txt)")
        if not files:
            return
        self.worker = PopulateWorker(files)
        self.worker.progress.connect(self.progress.setValue)
        self.worker.status_update.connect(self.update_log)
        self.worker.finished.connect(self.population_finished)
        self.worker.start()
        self.load_button.setEnabled(False)

    def update_log(self, message):
        self.log.append(message)

    def population_finished(self):
        self.log.append("Processing Complete")
        self.load_button.setEnabled(True)

    def parse_thrall_file(self, file_path):
        data = {}
        with open(file_path, "r", encoding='utf-8') as file:
            for line in file:
                if "=" in line:
                    key, value = line.strip().split("=", 1)
                    data[key.strip().lower()] = value.strip() or "N/A"
        return data

    def insert_or_update_data(self, data):
        conn = sqlite3.connect("thralls.db")
        cursor = conn.cursor()
        thrall_class = data.get("class", "").lower()
        if thrall_class not in [
            "alchemist", "archer", "armorer", "bearer", "blacksmith",
            "carpenter", "cook", "fighter", "performer", "priest",
            "smelter", "sorcerer", "tanner", "taskmaster", "pet", "animal"
        ]:
            conn.close()
            return f"INVALID CLASS: {thrall_class.upper()}"
        table_name = thrall_class.title() if thrall_class not in ["pet", "animal"] else ("Pets" if thrall_class == "pet" else "Animals")
        cursor.execute(f"""
            INSERT INTO {table_name} (
                name, id, class, health, strength, agility, vitality, grit,
                bonus_vitality, level_rate, armor, incoming_damage_reduction,
                killed_xp, temperament, gender, thrallable, race, faction,
                description, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET
                id=excluded.id,
                health=excluded.health,
                strength=excluded.strength,
                agility=excluded.agility,
                vitality=excluded.vitality,
                grit=excluded.grit,
                bonus_vitality=excluded.bonus_vitality,
                level_rate=excluded.level_rate,
                armor=excluded.armor,
                incoming_damage_reduction=excluded.incoming_damage_reduction,
                killed_xp=excluded.killed_xp,
                temperament=excluded.temperament,
                gender=excluded.gender,
                thrallable=excluded.thrallable,
                race=excluded.race,
                faction=excluded.faction,
                description=excluded.description,
                notes=excluded.notes
        """, (
            data.get("name", "N/A"), data.get("id", "N/A"), data.get("class", "N/A"),
            data.get("health", "N/A"), data.get("strength", "N/A"),
            data.get("agility", "N/A"), data.get("vitality", "N/A"),
            data.get("grit", "N/A"), data.get("bonus vitality", "N/A"),
            data.get("level rate", "N/A"), data.get("armor", "N/A"),
            data.get("incoming damage reduction", "N/A"), data.get("killed xp", "N/A"),
            data.get("temperament", "N/A"), data.get("gender", "N/A"),
            data.get("thrallable", "N/A"), data.get("race", "N/A"),
            data.get("faction", "N/A"), data.get("description", "N/A"),
            data.get("notes", "N/A")
        ))
        conn.commit()
        conn.close()
        return "SUCCESS"

# Initialize DB
def initialize_database():
    conn = sqlite3.connect("thralls.db")
    cursor = conn.cursor()
    classes = [
        "Alchemist", "Archer", "Armorer", "Bearer", "Blacksmith",
        "Carpenter", "Cook", "Fighter", "Performer", "Priest",
        "Smelter", "Sorcerer", "Tanner", "Taskmaster", "Pets", "Animals"
    ]
    for thrall_class in classes:
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {thrall_class} (
                name TEXT PRIMARY KEY,
                id TEXT,
                class TEXT,
                health TEXT,
                strength TEXT,
                agility TEXT,
                vitality TEXT,
                grit TEXT,
                bonus_vitality TEXT,
                level_rate TEXT,
                armor TEXT,
                incoming_damage_reduction TEXT,
                killed_xp TEXT,
                temperament TEXT,
                gender TEXT,
                thrallable TEXT,
                race TEXT,
                faction TEXT,
                description TEXT,
                notes TEXT
            )
        """)
    conn.commit()
    conn.close()

# Main App
class ConanWikiToolsApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Conan Wiki Tools")
        self.setGeometry(100, 100, 1200, 800)

        self.tabs = QTabWidget()
        self.tabs.addTab(ScraperTab(), "Scraper")
        self.tabs.addTab(FormatterTab(), "Formatter")
        self.tabs.addTab(DBPopulatorTab(), "DB Populator")
        self.tabs.addTab(DBViewerTab(), "DB Viewer")

        self.setCentralWidget(self.tabs)

        self.setStyleSheet(CONAN_STYLE)

if __name__ == "__main__":
    initialize_database()
    app = QApplication(sys.argv)
    window = ConanWikiToolsApp()
    window.show()
    sys.exit(app.exec())