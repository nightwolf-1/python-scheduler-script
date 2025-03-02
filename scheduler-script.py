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
from logging.handlers import RotatingFileHandler
from typing import Tuple, Dict, Any, Optional
import argparse


class ConfigManager:
    """Gère le chargement et la validation de la configuration."""
    
    @staticmethod
    def load_config(config_file: str) -> Dict[str, Any]:
        """Charge la configuration depuis un fichier JSON.
        
        Args:
            config_file: Chemin vers le fichier de configuration JSON
            
        Returns:
            Dictionnaire contenant la configuration
            
        Raises:
            FileNotFoundError: Si le fichier de configuration n'existe pas
            json.JSONDecodeError: Si le fichier n'est pas un JSON valide
        """
        try:
            with open(config_file, 'r') as f:
                config = json.load(f)
            
            # Validation de base
            required_fields = ['start_time', 'script', 'repeat_time', 'log_retention']
            for field in required_fields:
                if field not in config:
                    raise ValueError(f"Champ requis manquant dans la configuration: {field}")
            
            return config
        except FileNotFoundError:
            logging.error(f"Fichier de configuration non trouvé: {config_file}")
            raise
        except json.JSONDecodeError:
            logging.error(f"Format JSON invalide dans le fichier: {config_file}")
            raise


class LogManager:
    """Gère la configuration et la rotation des logs."""
    
    def __init__(self, log_dir: str, retention_days: int):
        """Initialise le gestionnaire de logs.
        
        Args:
            log_dir: Répertoire où stocker les fichiers de logs
            retention_days: Nombre de jours de rétention des logs
        """
        self.log_dir = log_dir
        self.retention_days = retention_days
        os.makedirs(log_dir, exist_ok=True)
        
        # Configuration du logger principal
        self.setup_main_logger()
        
        # Date de dernière vérification des logs
        self.last_cleanup_check = datetime.datetime.now()
        
    def setup_main_logger(self) -> None:
        """Configure le logger principal de l'application."""
        log_file = os.path.join(self.log_dir, f"scheduler_{datetime.datetime.now().strftime('%Y-%m-%d')}.log")
        
        # Configuration du handler de fichier avec rotation
        file_handler = RotatingFileHandler(
            filename=log_file,
            maxBytes=5*1024*1024,  # 5 MB
            backupCount=5
        )
        
        # Configuration du format de log
        log_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(log_format)
        
        # Configuration du logger racine
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.INFO)
        root_logger.addHandler(file_handler)
        
        # Ajout d'un handler pour la console
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(log_format)
        root_logger.addHandler(console_handler)
    
    def get_job_log_file(self, script_name: str) -> str:
        """Crée un fichier de log spécifique pour un script.
        
        Args:
            script_name: Nom du script pour lequel créer un fichier de log
            
        Returns:
            Chemin vers le fichier de log
        """
        script_base = os.path.basename(script_name).replace('.py', '')
        return os.path.join(self.log_dir, f"{script_base}_{datetime.datetime.now().strftime('%Y-%m-%d')}.log")
    
    def clear_old_logs(self, force: bool = False) -> None:
        """Supprime les fichiers de log plus anciens que la période de rétention.
        
        Args:
            force: Si True, effectue le nettoyage indépendamment de la dernière vérification
        """
        now = datetime.datetime.now()
        
        # Vérification quotidienne uniquement, sauf si force=True
        if not force and (now - self.last_cleanup_check).total_seconds() < 86400:  # 24 heures
            return
            
        self.last_cleanup_check = now
        retention_period = datetime.timedelta(days=self.retention_days)
        
        logging.info(f"Vérification des anciens fichiers de log (rétention: {self.retention_days} jours)")
        
        try:
            for filename in os.listdir(self.log_dir):
                file_path = os.path.join(self.log_dir, filename)
                if os.path.isfile(file_path):
                    file_creation_time = datetime.datetime.fromtimestamp(os.path.getctime(file_path))
                    if now - file_creation_time > retention_period:
                        os.remove(file_path)
                        logging.info(f"Fichier de log supprimé: {file_path}")
        except Exception as e:
            logging.error(f"Erreur lors du nettoyage des logs: {e}")


class ScriptExecutor:
    """Gère l'exécution des scripts Python."""
    
    def __init__(self, log_manager: LogManager):
        """Initialise l'exécuteur de scripts.
        
        Args:
            log_manager: Gestionnaire de logs à utiliser
        """
        self.log_manager = log_manager
    
    @staticmethod
    def clear_console() -> None:
        """Efface la console en fonction du système d'exploitation."""
        if os.name == 'nt':
            os.system('cls')  # Pour Windows
        else:
            os.system('clear')  # Pour Unix/Linux/Mac
    
    def execute(self, script: str, python_exec: str = "python", venv: Optional[str] = None) -> bool:
        """Exécute un script Python.
        
        Args:
            script: Chemin vers le script Python à exécuter
            python_exec: Exécutable Python à utiliser
            venv: Chemin optionnel vers un environnement virtuel
            
        Returns:
            True si l'exécution s'est terminée avec succès, False sinon
        """
        self.clear_console()
        current_time = datetime.datetime.now()
        log_file = self.log_manager.get_job_log_file(script)
        
        logging.info(f"Lancement du script Python {script}...")
        
        # Préparation de la commande
        if venv:
            if os.name == 'nt':
                python_path = os.path.join(venv, 'Scripts', 'python.exe')
            else:
                python_path = os.path.join(venv, 'bin', 'python')
        else:
            python_path = python_exec
        
        cmd = [python_path, script]
        
        # Exécution du script
        try:
            with open(log_file, 'a') as f:
                f.write(f"[{current_time}] Lancement du script Python {script}...\n")
                
                result = subprocess.run(
                    cmd, 
                    check=True, 
                    stdout=subprocess.PIPE, 
                    stderr=subprocess.PIPE, 
                    text=True
                )
                
                f.write(result.stdout)
                f.write(result.stderr)
                
                logging.info(f"Exécution réussie de {script}")
                logging.debug(result.stdout)
                
                if result.stderr:
                    logging.warning(f"Messages d'erreur de {script}: {result.stderr}")
                
                return True
                
        except subprocess.CalledProcessError as e:
            error_message = f"Erreur lors de l'exécution du script {script}: {e}"
            
            with open(log_file, 'a') as f:
                f.write(f"{error_message}\n")
                f.write(e.stderr)
            
            logging.error(error_message)
            logging.error(e.stderr)
            
            return False
        except Exception as e:
            logging.error(f"Exception inattendue lors de l'exécution de {script}: {e}")
            return False


class Scheduler:
    """Gère la planification et l'exécution périodique des scripts."""
    
    def __init__(self, executor: ScriptExecutor, log_manager: LogManager):
        """Initialise le planificateur.
        
        Args:
            executor: Exécuteur de scripts à utiliser
            log_manager: Gestionnaire de logs à utiliser
        """
        self.executor = executor
        self.log_manager = log_manager
        self.scheduled_jobs = []
        self.state_file = os.path.join(log_manager.log_dir, "scheduler_state.json")
        
    def parse_interval(self, repeat_time: str) -> datetime.timedelta:
        """Parse une chaîne d'intervalle et la convertit en timedelta.
        
        Args:
            repeat_time: Chaîne de caractères représentant l'intervalle (ex: "1h", "30m", "45s")
            
        Returns:
            Objet timedelta correspondant à l'intervalle
            
        Raises:
            ValueError: Si le format de l'intervalle est invalide
        """
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
    
    def schedule_job(self, start_time_str: str, repeat_time: str, script: str, python_exec: str = "python", venv: Optional[str] = None) -> Tuple[datetime.datetime, datetime.timedelta]:
        """Planifie l'exécution périodique d'un script.
        
        Args:
            start_time_str: Heure de début au format "HH:MM:SS"
            repeat_time: Intervalle de répétition (ex: "1h", "30m")
            script: Chemin vers le script à exécuter
            python_exec: Exécutable Python à utiliser
            venv: Chemin optionnel vers un environnement virtuel
            
        Returns:
            Tuple contenant la prochaine heure d'exécution et l'intervalle
        """
        # Normalisation de 24:00:00 à 00:00:00
        if start_time_str == "24:00:00":
            start_time_str = "00:00:00"

        # Conversion de l'intervalle en timedelta
        interval = self.parse_interval(repeat_time)
        
        # Conversion de l'heure de début
        try:
            start_time = datetime.datetime.strptime(start_time_str, "%H:%M:%S").time()
        except ValueError:
            raise ValueError(f"Format d'heure invalide: {start_time_str}. Utilisez le format HH:MM:SS")

        # Calcul de la prochaine exécution
        def calculate_next_run() -> datetime.datetime:
            now = datetime.datetime.now()
            first_run = datetime.datetime.combine(now.date(), start_time)
            while first_run <= now:
                first_run += interval
            return first_run

        next_run_time = calculate_next_run()
        
        # Création de la tâche récurrente
        job_info = {
            'script': script,
            'python_exec': python_exec,
            'venv': venv,
            'interval': interval,
            'next_run': next_run_time
        }
        
        def job_wrapper():
            nonlocal job_info
            # Exécution du script
            self.executor.execute(job_info['script'], job_info['python_exec'], job_info['venv'])
            
            # Mise à jour de la prochaine exécution
            job_info['next_run'] += job_info['interval']
            
            # Planification de la prochaine exécution
            schedule.every().day.at(job_info['next_run'].strftime("%H:%M:%S")).do(job_wrapper).tag(job_info['script'])
            
            # Nettoyage des anciennes tâches pour éviter l'accumulation
            schedule.clear(tag=job_info['script'])
            
            # Sauvegarde de l'état
            self.save_state()

        # Planification de la première exécution
        schedule.every().day.at(next_run_time.strftime("%H:%M:%S")).do(job_wrapper).tag(script)
        
        # Ajout au suivi des tâches planifiées
        self.scheduled_jobs.append(job_info)
        
        logging.info(f"Le script {script} commencera à {start_time_str} et sera répété toutes les {repeat_time}")
        
        return next_run_time, interval
    
    def get_next_run_time(self) -> Optional[datetime.datetime]:
        """Obtient l'heure de la prochaine exécution planifiée.
        
        Returns:
            Heure de la prochaine exécution, ou None si aucune tâche n'est planifiée
        """
        if not self.scheduled_jobs:
            return None
            
        return min(job['next_run'] for job in self.scheduled_jobs)
    
    def save_state(self) -> None:
        """Sauvegarde l'état actuel du planificateur."""
        state = []
        for job in self.scheduled_jobs:
            state.append({
                'script': job['script'],
                'python_exec': job['python_exec'],
                'venv': job['venv'],
                'interval_seconds': job['interval'].total_seconds(),
                'next_run': job['next_run'].isoformat()
            })
        
        try:
            with open(self.state_file, 'w') as f:
                json.dump(state, f)
        except Exception as e:
            logging.error(f"Erreur lors de la sauvegarde de l'état: {e}")
    
    def load_state(self) -> bool:
        """Charge l'état précédemment sauvegardé.
        
        Returns:
            True si l'état a été chargé avec succès, False sinon
        """
        if not os.path.exists(self.state_file):
            return False
            
        try:
            with open(self.state_file, 'r') as f:
                state = json.load(f)
                
            for job_state in state:
                interval = datetime.timedelta(seconds=job_state['interval_seconds'])
                next_run = datetime.datetime.fromisoformat(job_state['next_run'])
                
                # Si la prochaine exécution est dépassée, recalculer
                now = datetime.datetime.now()
                while next_run < now:
                    next_run += interval
                
                job_info = {
                    'script': job_state['script'],
                    'python_exec': job_state['python_exec'],
                    'venv': job_state['venv'],
                    'interval': interval,
                    'next_run': next_run
                }
                
                def job_wrapper():
                    nonlocal job_info
                    self.executor.execute(job_info['script'], job_info['python_exec'], job_info['venv'])
                    job_info['next_run'] += job_info['interval']
                    schedule.every().day.at(job_info['next_run'].strftime("%H:%M:%S")).do(job_wrapper).tag(job_info['script'])
                    schedule.clear(tag=job_info['script'])
                    self.save_state()
                
                schedule.every().day.at(next_run.strftime("%H:%M:%S")).do(job_wrapper).tag(job_state['script'])
                self.scheduled_jobs.append(job_info)
                
                logging.info(f"Tâche restaurée: {job_state['script']}, prochaine exécution: {next_run}")
                
            return True
            
        except Exception as e:
            logging.error(f"Erreur lors du chargement de l'état: {e}")
            return False


def parse_arguments():
    """Parse les arguments de ligne de commande.
    
    Returns:
        Namespace contenant les arguments parsés
    """
    parser = argparse.ArgumentParser(description="Planificateur de scripts Python")
    
    # Deux modes principaux: fichier de configuration ou arguments en ligne de commande
    mode_group = parser.add_mutually_exclusive_group(required=True)
    
    mode_group.add_argument('--config', 
                        help='Chemin vers un fichier de configuration JSON')
    
    mode_group.add_argument('--start-time', 
                        help='Heure de démarrage au format HH:MM:SS')
    
    # Arguments pour le mode ligne de commande
    parser.add_argument('--script', 
                       help='Chemin vers le script Python à exécuter')
    
    parser.add_argument('--repeat-time', 
                       help='Intervalle de répétition (ex: 1h, 30m, 45s)')
    
    parser.add_argument('--log-retention', type=int, default=30,
                       help='Nombre de jours de conservation des logs (défaut: 30)')
    
    parser.add_argument('--python-exec', default='python',
                       help='Exécutable Python à utiliser (défaut: python)')
    
    parser.add_argument('--venv',
                       help='Chemin vers un environnement virtuel à utiliser')
    
    args = parser.parse_args()
    
    # Validation des arguments requis en mode ligne de commande
    if args.start_time and not (args.script and args.repeat_time):
        parser.error("--script et --repeat-time sont requis avec --start-time")
    
    return args


def setup_signal_handlers(scheduler):
    """Configure les gestionnaires de signaux pour une sortie propre.
    
    Args:
        scheduler: Instance du planificateur à utiliser
    """
    def graceful_exit(signum, frame):
        logging.info("Arrêt du programme en cours...")
        scheduler.save_state()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, graceful_exit)
    signal.signal(signal.SIGTERM, graceful_exit)


def main():
    """Point d'entrée principal du programme."""
    # Parsing des arguments
    args = parse_arguments()
    
    # Configuration à partir des arguments ou du fichier de configuration
    if args.config:
        try:
            config = ConfigManager.load_config(args.config)
            start_time_str = config['start_time']
            script = config['script']
            repeat_time = config['repeat_time']
            log_retention_days = int(config['log_retention'])
            python_exec = config.get('python_exec', 'python')
            venv = config.get('venv')
        except Exception as e:
            print(f"Erreur lors du chargement de la configuration: {e}")
            sys.exit(1)
    else:
        start_time_str = args.start_time
        script = args.script
        repeat_time = args.repeat_time
        log_retention_days = args.log_retention
        python_exec = args.python_exec
        venv = args.venv
    
    # Initialisation des composants
    log_dir = "logs"
    log_manager = LogManager(log_dir, log_retention_days)
    executor = ScriptExecutor(log_manager)
    scheduler = Scheduler(executor, log_manager)
    
    # Configuration des gestionnaires de signaux
    setup_signal_handlers(scheduler)
    
    # Tentative de chargement de l'état précédent
    if not scheduler.load_state():
        # Planification initiale si aucun état n'a été chargé
        scheduler.schedule_job(start_time_str, repeat_time, script, python_exec, venv)
    
    # Boucle principale
    try:
        logging.info("Démarrage du planificateur")
        
        while True:
            schedule.run_pending()
            
            next_run_time = scheduler.get_next_run_time()
            if next_run_time:
                next_run_time_str = next_run_time.strftime('%Y-%m-%d %H:%M:%S')
                sys.stdout.write(f"\rLe prochain script sera lancé à {next_run_time_str}. Ctrl+C pour quitter.")
                sys.stdout.flush()
            
            # Vérification des logs anciens (une fois par jour)
            log_manager.clear_old_logs()
            
            time.sleep(1)
            
    except Exception as e:
        logging.critical(f"Erreur fatale: {e}")
        scheduler.save_state()
        sys.exit(1)


if __name__ == "__main__":
    main()