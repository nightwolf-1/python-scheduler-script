import schedule
import time
import subprocess
import sys
import re
import datetime
import signal
import os
import json
import logging
import sqlite3
from logging.handlers import RotatingFileHandler
from typing import Tuple, Dict, Any, Optional, List
import argparse
import shlex
import pathlib
import uuid  # pour générer automatiquement un identifiant unique


class DatabaseManager:
    """Gère les opérations de base de données pour le planificateur."""
    
    def __init__(self, db_path: str = "scheduler.db"):
        self.db_path = db_path
        self.initialize_db()
    
    def get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn
    
    def initialize_db(self) -> None:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            working_dir TEXT NOT NULL,
            script TEXT NOT NULL,
            python_exec TEXT NOT NULL,
            venv TEXT,
            start_time TEXT NOT NULL,
            repeat_time TEXT NOT NULL,
            next_run TEXT NOT NULL,
            interval_seconds INTEGER NOT NULL,
            log_retention INTEGER,
            log_path TEXT,
            active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        ''')
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS job_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id TEXT NOT NULL,
            start_time TEXT NOT NULL,
            end_time TEXT,
            status TEXT NOT NULL,
            log_file TEXT,
            FOREIGN KEY (job_id) REFERENCES jobs(id)
        )
        ''')
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS config (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        ''')
        # Insertion des valeurs par défaut pour la configuration si elles n'existent pas
        cursor.execute("SELECT value FROM config WHERE key = 'log_retention'")
        if not cursor.fetchone():
            cursor.execute("INSERT INTO config (key, value) VALUES ('log_retention', '7')")
        
        cursor.execute("SELECT value FROM config WHERE key = 'log_dir'")
        if not cursor.fetchone():
            cursor.execute("INSERT INTO config (key, value) VALUES ('log_dir', 'logs')")
        conn.commit()
        conn.close()
    
    def save_job(self, job_data: Dict[str, Any]) -> None:
        conn = self.get_connection()
        cursor = conn.cursor()
        now = datetime.datetime.now().isoformat()
        # Champs additionnels
        lr = job_data.get("log_retention")
        log_path = job_data.get("log_path")
        working_dir = job_data.get("working_dir")

        cursor.execute("SELECT id FROM jobs WHERE id = ?", (job_data['id'],))
        exists = cursor.fetchone()
        if exists:
            cursor.execute('''
            UPDATE jobs SET
                name = ?,
                script = ?,
                python_exec = ?,
                venv = ?,
                working_dir = ?,
                start_time = ?,
                repeat_time = ?,
                next_run = ?,
                interval_seconds = ?,
                log_retention = ?,
                log_path = ?,
                active = ?,
                updated_at = ?
            WHERE id = ?
            ''', (
                job_data['name'],
                job_data['script'],
                job_data['python_exec'],
                job_data['venv'],
                working_dir,
                job_data['start_time'],
                job_data['repeat_time'],
                job_data['next_run'].isoformat(),
                job_data['interval_seconds'],
                lr,
                log_path,
                job_data['active'],
                now,
                job_data['id']
            ))
        else:
            cursor.execute('''
            INSERT INTO jobs (
                id, name, script, python_exec, venv, working_dir, start_time, repeat_time,
                next_run, interval_seconds, log_retention, log_path, active, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                job_data['id'],
                job_data['name'],
                job_data['script'],
                job_data['python_exec'],
                job_data['venv'],
                working_dir,
                job_data['start_time'],
                job_data['repeat_time'],
                job_data['next_run'].isoformat(),
                job_data['interval_seconds'],
                lr,
                log_path,
                job_data['active'],
                now,
                now
            ))
        conn.commit()
        conn.close()
    
    def get_all_jobs(self) -> List[Dict[str, Any]]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM jobs WHERE active = 1")
        jobs = []
        for row in cursor.fetchall():
            job = dict(row)
            job['next_run'] = datetime.datetime.fromisoformat(job['next_run'])
            jobs.append(job)
        conn.close()
        return jobs
    
    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM jobs WHERE id = ? AND active = 1", (job_id,))
        row = cursor.fetchone()
        conn.close()
        if row:
            job = dict(row)
            job['next_run'] = datetime.datetime.fromisoformat(job['next_run'])
            return job
        return None
    
    def deactivate_job(self, job_id: str) -> bool:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("UPDATE jobs SET active = 0, updated_at = ? WHERE id = ?", 
                     (datetime.datetime.now().isoformat(), job_id))
        updated = cursor.rowcount > 0
        conn.commit()
        conn.close()
        return updated
    
    def record_job_run(self, job_id: str, log_file: str) -> int:
        conn = self.get_connection()
        cursor = conn.cursor()
        now = datetime.datetime.now().isoformat()
        cursor.execute('''
        INSERT INTO job_runs (job_id, start_time, status, log_file)
        VALUES (?, ?, ?, ?)
        ''', (job_id, now, 'running', log_file))
        run_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return run_id
    
    def update_job_run(self, run_id: int, status: str) -> None:
        conn = self.get_connection()
        cursor = conn.cursor()
        now = datetime.datetime.now().isoformat()
        cursor.execute('''
        UPDATE job_runs SET status = ?, end_time = ? WHERE id = ?
        ''', (status, now, run_id))
        conn.commit()
        conn.close()

    def get_config(self, key: str, default: str = None) -> str:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT value FROM config WHERE key = ?", (key,))
        row = cursor.fetchone()
        conn.close()
        return row['value'] if row else default
    
    def set_config(self, key: str, value: str) -> None:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT value FROM config WHERE key = ?", (key,))
        if cursor.fetchone():
            cursor.execute("UPDATE config SET value = ? WHERE key = ?", (value, key))
        else:
            cursor.execute("INSERT INTO config (key, value) VALUES (?, ?)", (key, value))
        conn.commit()
        conn.close()


class ConfigManager:
    """Gère le chargement et la validation de la configuration."""
    
    @staticmethod
    def load_config(config_file: str) -> Dict[str, Any]:
        try:
            with open(config_file, 'r') as f:
                config = json.load(f)
            # Les champs requis incluent désormais log_retention
            required_fields = ['start_time', 'script', 'repeat_time', 'log_retention', 'name']
            for field in required_fields:
                if field not in config:
                    raise ValueError(f"Champ requis manquant dans la configuration: {field}")
            # Si un répertoire de travail est spécifié, on valide le chemin complet
            if 'working_dir' in config:
                script_path = os.path.join(config['working_dir'], config['script'])
                ConfigManager.validate_script_path(script_path)
            else:
                ConfigManager.validate_script_path(config['script'])
            return config
        except FileNotFoundError:
            logging.error(f"Fichier de configuration non trouvé: {config_file}")
            raise
        except json.JSONDecodeError:
            logging.error(f"Format JSON invalide dans le fichier: {config_file}")
            raise
    
    @staticmethod
    def validate_script_path(script_path: str) -> bool:
        normalized_path = os.path.normpath(script_path)
        dangerous_chars = ['|', '&', ';', '$', '>', '<', '`', '\\']
        for char in dangerous_chars:
            if char in script_path:
                raise ValueError(f"Chemin de script invalide: caractère non autorisé '{char}'")
        if not normalized_path.endswith('.py'):
            raise ValueError("Le script doit avoir l'extension .py")
        if not os.path.isfile(normalized_path):
            raise ValueError(f"Le script n'existe pas: {normalized_path}")
        return True


class LogManager:
    """Gère la configuration et la rotation des logs."""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.log_dir = self.db_manager.get_config('log_dir', 'logs')
        self.global_retention = int(self.db_manager.get_config('log_retention', '7'))
        os.makedirs(self.log_dir, exist_ok=True)
        self.setup_main_logger()
        self.last_cleanup_check = datetime.datetime.now()
    
    def setup_main_logger(self) -> None:
        log_file = os.path.join(self.log_dir, f"scheduler_{datetime.datetime.now().strftime('%Y-%m-%d')}.log")
        file_handler = RotatingFileHandler(
            filename=log_file,
            maxBytes=5 * 1024 * 1024,
            backupCount=5
        )
        log_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(log_format)
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.INFO)
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
        root_logger.addHandler(file_handler)
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(log_format)
        root_logger.addHandler(console_handler)
    
    def get_job_log_directory(self, job_id: str, job_name: str, repeat_time: str, retention: Optional[int] = None) -> str:
        job = self.db_manager.get_job(job_id)
        if job and job.get('log_path'):
            job_dir = job['log_path']
            # S'assurer que le répertoire existe
            os.makedirs(job_dir, exist_ok=True)
            return job_dir
        # Nettoie le nom de la tâche : remplace les espaces par des underscores et tronque à 30 caractères
        sanitized = "".join(c if c.isalnum() or c in ('_', '-') else '_' for c in job_name).lower()
        if len(sanitized) > 30:
            sanitized = sanitized[:30]
        base_dir_name = f"{sanitized}_{repeat_time}"
        job_dir = os.path.join(self.log_dir, base_dir_name)
        # Si le répertoire existe déjà, ajoute un suffixe numérique
        if os.path.exists(job_dir):
            suffix = 1
            while os.path.exists(f"{job_dir}{suffix}"):
                suffix += 1
            job_dir = f"{job_dir}{suffix}"
        os.makedirs(job_dir, exist_ok=True)
        return job_dir
    
    def get_job_log_file(self, job_id: str ,job_name: str, repeat_time: str, retention: Optional[int] = None) -> str:
        # Utilise le répertoire dédié au job pour créer le fichier de log
        job_dir = self.get_job_log_directory(job_id, job_name, repeat_time, retention)
        sanitized = "".join(c if c.isalnum() or c in ('_', '-') else '_' for c in job_name).lower()
        if len(sanitized) > 30:
            sanitized = sanitized[:30]
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        filename = f"{sanitized}_{timestamp}.log"
        return os.path.join(job_dir, filename)
    
    def clear_old_logs(self, force: bool = False) -> None:
        now = datetime.datetime.now()
        if not force and (now - self.last_cleanup_check).total_seconds() < 86400:
            return
        self.last_cleanup_check = now
        logging.info("Lancement du nettoyage des logs...")
        try:
            # Parcours de toutes les sous-arborescences de self.log_dir
            for root, _, files in os.walk(self.log_dir):
                # Vérifie si le répertoire contient un fichier retention.txt
                retention_file = os.path.join(root, "retention.txt")
                if os.path.isfile(retention_file):
                    with open(retention_file, "r") as f:
                        try:
                            retention_days = int(f.read().strip())
                        except Exception:
                            retention_days = self.global_retention
                else:
                    retention_days = self.global_retention
                retention_period = datetime.timedelta(days=retention_days)
                for filename in files:
                    file_path = os.path.join(root, filename)
                    file_creation_time = datetime.datetime.fromtimestamp(os.path.getctime(file_path))
                    if now - file_creation_time > retention_period:
                        os.remove(file_path)
                        logging.info(f"Fichier de log supprimé: {file_path}")
        except Exception as e:
            logging.error(f"Erreur lors du nettoyage des logs: {e}")


class ScriptExecutor:
    """Gère l'exécution des scripts Python."""
    
    def __init__(self, log_manager: LogManager, db_manager: DatabaseManager):
        self.log_manager = log_manager
        self.db_manager = db_manager
    
    @staticmethod
    def clear_console() -> None:
        if os.name == 'nt':
            os.system('cls')
        else:
            os.system('clear')
    
    @staticmethod
    def secure_command(script: str, python_exec: str, working_dir: Optional[str] = None) -> List[str]:
        if working_dir:
            script_path = pathlib.Path(os.path.join(working_dir, script)).resolve()
        else:
            script_path = pathlib.Path(script).resolve()
            
        # Utiliser directement l'exécutable Python sans appliquer shlex.quote
        python_path = python_exec
        if not script_path.exists() or script_path.suffix != '.py':
            raise ValueError(f"Script invalide: {script}")
        return [python_path, str(script_path)]
    
    def execute(self, job_id: str, job_name: str, script: str, repeat_time: str, 
                python_exec: str = "python", venv: Optional[str] = None, 
                working_dir: Optional[str] = None) -> bool:
        self.clear_console()
        current_time = datetime.datetime.now()
        job = self.db_manager.get_job(job_id)
        retention = job.get('log_retention') if job else None
        log_file = self.log_manager.get_job_log_file(job_id, job_name, repeat_time, retention)
        logging.info(f"Lancement du script Python {script} (Job ID: {job_id} - {job_name})...")
        run_id = self.db_manager.record_job_run(job_id, log_file)
        try:
            if venv:
                if os.name == 'nt':
                    python_path = os.path.join(venv, 'Scripts', 'python.exe')
                else:
                    python_path = os.path.join(venv, 'bin', 'python')
            else:
                python_path = python_exec
            cmd = self.secure_command(script, python_path, working_dir)
            
            # Définir le répertoire de travail pour subprocess.run
            subprocess_cwd = working_dir if working_dir else None
            
            with open(log_file, 'a') as f:
                f.write(f"[{current_time}] Lancement du script Python {script} (Job ID: {job_id} - {job_name})...\n")
                if working_dir:
                    f.write(f"Répertoire de travail: {working_dir}\n")
                
                result = subprocess.run(
                    cmd,
                    check=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    cwd=subprocess_cwd
                )
                f.write(result.stdout)
                f.write(result.stderr)
                logging.info(f"Exécution réussie de {script} (Job ID: {job_id} - {job_name})")
                logging.debug(result.stdout)
                if result.stderr:
                    logging.warning(f"Messages d'erreur de {script}: {result.stderr}")
                self.db_manager.update_job_run(run_id, 'success')
                return True
        except subprocess.CalledProcessError as e:
            error_message = f"Erreur lors de l'exécution du script {script} (Job ID: {job_id} - {job_name}): {e}"
            with open(log_file, 'a') as f:
                f.write(f"{error_message}\n")
                f.write(e.stderr)
            logging.error(error_message)
            logging.error(e.stderr)
            self.db_manager.update_job_run(run_id, 'error')
            return False
        except Exception as e:
            logging.error(f"Exception inattendue lors de l'exécution de {script} (Job ID: {job_id} - {job_name}): {e}")
            self.db_manager.update_job_run(run_id, 'error')
            return False


class Scheduler:
    """Gère la planification et l'exécution périodique des scripts."""
    
    def __init__(self, executor: ScriptExecutor, log_manager: LogManager, db_manager: DatabaseManager):
        self.executor = executor
        self.log_manager = log_manager
        self.db_manager = db_manager
    
    def parse_interval(self, repeat_time: str) -> datetime.timedelta:
        match = re.match(r"(\d+)([hms])", repeat_time)
        if not match:
            raise ValueError("Intervalle invalide. Utilisez le format <durée><unité> (ex: 1h, 2m, 30s)")
        value, unit = int(match.group(1)), match.group(2)
        if unit == "h":
            return datetime.timedelta(hours=value)
        elif unit == "m":
            return datetime.timedelta(minutes=value)
        elif unit == "s":
            return datetime.timedelta(seconds=value)
        else:
            raise ValueError("Unité invalide. Utilisez 'h' pour heures, 'm' pour minutes, ou 's' pour secondes.")
    
    def schedule_job(self, job_id: str, job_name: str, start_time_str: str, repeat_time: str, 
                     script: str, python_exec: str = "python", venv: Optional[str] = None, 
                     log_retention: Optional[int] = None, working_dir: Optional[str] = None) -> Tuple[datetime.datetime, datetime.timedelta]:
        if start_time_str == "24:00:00":
            start_time_str = "00:00:00"
        interval = self.parse_interval(repeat_time)
        try:
            start_time = datetime.datetime.strptime(start_time_str, "%H:%M:%S").time()
        except ValueError:
            raise ValueError(f"Format d'heure invalide: {start_time_str}. Utilisez le format HH:MM:SS")
        def calculate_next_run() -> datetime.datetime:
            now = datetime.datetime.now()
            first_run = datetime.datetime.combine(now.date(), start_time)
            while first_run <= now:
                first_run += interval
            return first_run
        next_run_time = calculate_next_run()
        existing_job = self.db_manager.get_job(job_id)
        log_path = existing_job.get('log_path') if existing_job else None

        if not log_path:
            log_path = self.log_manager.get_job_log_directory(job_id, job_name, repeat_time, log_retention)
        job_info = {
            'id': job_id,
            'name': job_name,
            'script': script,
            'python_exec': python_exec,
            'venv': venv,
            'start_time': start_time_str,
            'repeat_time': repeat_time,
            'interval': interval,
            'interval_seconds': int(interval.total_seconds()),
            'next_run': next_run_time,
            'log_retention': log_retention,
            'log_path': log_path,
            'working_dir': working_dir,
            'active': 1
        }
        def job_wrapper():
            # Exécute la tâche et planifie la prochaine exécution
            self.executor.execute(job_info['id'], job_info['name'], job_info['script'], job_info['repeat_time'], job_info['python_exec'], job_info['venv'], job_info['working_dir'])
            job_info['next_run'] += job_info['interval']
            self.db_manager.save_job(job_info)
            schedule.clear(tag=job_info['id'])
            schedule.every().day.at(job_info['next_run'].strftime("%H:%M:%S")).do(job_wrapper).tag(job_info['id'])
            logging.info(f"Job '{job_info['name']}' prochaine exécution prévue à {job_info['next_run']}")
        schedule.every().day.at(next_run_time.strftime("%H:%M:%S")).do(job_wrapper).tag(job_id)
        self.db_manager.save_job(job_info)
        logging.info(f"Job ID: {job_id} - '{job_name}' planifié: le script {script} commencera à {start_time_str}, sera répété toutes les {repeat_time} et prochaine exécution à {next_run_time}")
        return next_run_time, interval


def parse_arguments():
    parser = argparse.ArgumentParser(description="Planificateur de scripts Python")
    # Option globale pour log_retention
    parser.add_argument("--log_retention", type=int, default=7, help="Nombre de jours de rétention des logs global (défaut: 7)")
    
    subparsers = parser.add_subparsers(dest='command', help='Commande à exécuter')
    
    # Commande 'add'
    add_parser = subparsers.add_parser('add', help='Ajouter un nouveau job')
    add_parser.add_argument('--config', help='Chemin vers un fichier de configuration JSON')
    add_parser.add_argument('--id', help='Identifiant unique du job (optionnel, sera généré automatiquement si absent)')
    add_parser.add_argument('--name', help='Nom descriptif du job')
    add_parser.add_argument('--script', help='Chemin vers le script Python à exécuter')
    add_parser.add_argument('--python_exec', default='python', help='Exécutable Python (défaut: python)')
    add_parser.add_argument('--venv', help="Chemin vers l'environnement virtuel (optionnel)")
    add_parser.add_argument('--start_time', help='Heure de début au format HH:MM:SS')
    add_parser.add_argument('--repeat_time', help='Intervalle de répétition (ex: 1h, 30m, 45s)')
    add_parser.add_argument('--working_dir', help="Répertoire de travail pour l'exécution du script (pour les chemins relatifs)")
    
    # Commande 'mod' pour modifier un job existant
    mod_parser = subparsers.add_parser('mod', help='Modifier un job existant')
    mod_parser.add_argument('--id', help='Identifiant unique du job à modifier', required=True)
    mod_parser.add_argument('--name', help='Nouveau nom descriptif du job')
    mod_parser.add_argument('--script', help='Nouveau chemin vers le script Python à exécuter')
    mod_parser.add_argument('--python_exec', help='Nouvel exécutable Python')
    mod_parser.add_argument('--venv', help="Nouveau chemin vers l'environnement virtuel")
    mod_parser.add_argument('--start_time', help='Nouvelle heure de début au format HH:MM:SS')
    mod_parser.add_argument('--repeat_time', help='Nouvel intervalle de répétition (ex: 1h, 30m, 45s)')
    mod_parser.add_argument('--working_dir', help="Nouveau répertoire de travail pour l'exécution du script")
    
    # Commande 'list'
    subparsers.add_parser('list', help='Lister tous les jobs planifiés (chargés depuis la base de données)')
    
    # Commande 'remove'
    remove_parser = subparsers.add_parser('remove', help='Supprimer un job')
    remove_parser.add_argument('--id', help='Identifiant unique du job à supprimer', required=True)
    
    # Commande 'show' pour afficher les paramètres d'un job
    show_parser = subparsers.add_parser('show', help='Afficher les paramètres d\'un job')
    show_parser.add_argument('--id', help='Identifiant unique du job à afficher', required=True)
    
    # Commande 'run'
    subparsers.add_parser('run', help='Démarrer le planificateur (chargera les jobs depuis la DB)')
    
    return parser.parse_args()


def main():
    args = parse_arguments()
    
    db_manager = DatabaseManager()

    # Valeur globale de rétention
    global_retention = args.log_retention

    db_manager.set_config('log_retention', str(args.log_retention))
    
    log_manager = LogManager(db_manager)
    executor = ScriptExecutor(log_manager, db_manager)
    scheduler_obj = Scheduler(executor, log_manager, db_manager)
    
    if args.command == 'add':
        if args.config:
            try:
                config = ConfigManager.load_config(args.config)
            except Exception as e:
                print(f"Erreur lors du chargement du fichier de configuration: {e}")
                sys.exit(1)
            job_id = str(uuid.uuid4())
            working_dir = config.get("working_dir")
            job_name = config.get("name")
            script = config.get("script")
            python_exec = config.get("python_exec", "python")
            venv = config.get("venv")
            start_time = config.get("start_time")
            repeat_time = config.get("repeat_time")
            # Utilise la rétention définie dans le profil
            profile_retention = int(config.get("log_retention", global_retention))
        else:
            missing = []
            for field in ['name', 'script', 'start_time', 'repeat_time']:
                if getattr(args, field) is None:
                    missing.append(field)
            if missing:
                print(f"Argument(s) requis manquant(s): {', '.join(missing)}")
                sys.exit(1)
            job_id = args.id if args.id else str(uuid.uuid4())
            working_dir = args.working_dir
            job_name = args.name
            script = args.script
            python_exec = args.python_exec
            venv = args.venv
            start_time = args.start_time
            repeat_time = args.repeat_time
            profile_retention = None  # N'utilise que la valeur globale
            
        try:
            next_run, interval = scheduler_obj.schedule_job(job_id, job_name, start_time, repeat_time, script, python_exec, venv, profile_retention, working_dir)
            print(f"Job ajouté: {job_id} - {job_name}, prochaine exécution à: {next_run}")
        except Exception as e:
            print(f"Erreur lors de la planification du job: {e}")
            sys.exit(1)
    
    elif args.command == 'mod':
        job = db_manager.get_job(args.id)
        if not job:
            print(f"Job avec l'ID {args.id} introuvable.")
            sys.exit(1)
        new_name = args.name if args.name is not None else job['name']
        new_script = args.script if args.script is not None else job['script']
        new_python_exec = args.python_exec if args.python_exec is not None else job['python_exec']
        new_venv = args.venv if args.venv is not None else job['venv']
        new_start_time = args.start_time if args.start_time is not None else job['start_time']
        new_repeat_time = args.repeat_time if args.repeat_time is not None else job['repeat_time']
        new_working_dir = args.working_dir if args.working_dir is not None else job.get("working_dir")
        new_retention = job.get("log_retention")
        try:
            next_run, interval = scheduler_obj.schedule_job(args.id, new_name, new_start_time, new_repeat_time, new_script, new_python_exec, new_venv, new_retention, new_working_dir)
            print(f"Job modifié: {args.id} - {new_name}, prochaine exécution à: {next_run}")
        except Exception as e:
            print(f"Erreur lors de la modification du job: {e}")
            sys.exit(1)
    
    elif args.command == 'list':
        jobs = db_manager.get_all_jobs()
        if not jobs:
            print("Aucun job planifié.")
        else:
            for job in jobs:
                print(f"ID: {job['id']}, Nom: {job['name']}, Prochaine exécution: {job['next_run']}")
    
    elif args.command == 'show':
        job = db_manager.get_job(args.id)
        if not job:
            print(f"Job avec l'ID {args.id} introuvable.")
        else:
            print("Détails du job:")
            for key, value in job.items():
                print(f"  {key}: {value}")
    
    elif args.command == 'remove':
        if db_manager.get_job(args.id):
            db_manager.deactivate_job(args.id)
            schedule.clear(tag=args.id)
            print(f"Job {args.id} supprimé avec succès.")
        else:
            print(f"Job {args.id} introuvable.")
    
    elif args.command == 'run':
        jobs = db_manager.get_all_jobs()
        logging.info(f"{len(jobs)} jobs chargés depuis la base de données.")
        print("Planificateur démarré. Appuyez sur Ctrl+C pour arrêter.")
        try:
            for job in jobs:
                scheduler_obj.schedule_job(job['id'], job['name'], job['start_time'], job['repeat_time'], job['script'], job['python_exec'], job['venv'], job.get("log_retention"), job.get("working_dir"))
            while True:
                schedule.run_pending()
                time.sleep(1)
                log_manager.clear_old_logs()
        except KeyboardInterrupt:
            print("Arrêt du planificateur.")
    
    else:
        print("Commande non reconnue. Utilisez --help pour voir les commandes disponibles.")


if __name__ == "__main__":
    main()
