import subprocess
import sys

# List of required packages
REQUIRED_PACKAGES = ['pandas', 'tqdm', 'googlemaps', 'datetime', 'openpyxl', 'concurrent.futures']

def install_package(package):
    """Install the given package using pip."""
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

def ensure_packages_installed(packages):
    """Ensure that all required packages are installed."""
    for package in packages:
        try:
            __import__(package)
        except ImportError:
            print(f"Package {package} not found. Installing...")
            install_package(package)

ensure_packages_installed(REQUIRED_PACKAGES)

# Import packages after installation
import pandas as pd
from tqdm import tqdm
import googlemaps
from datetime import datetime
import concurrent.futures

# Constants
GITHUB_URL = 'https://raw.githubusercontent.com/josevictorsl/challengecje/main/Fabricas.xlsx'
API_KEY_GOOGLE_MAPS = 'insira sua chave'
MAJOR_PORTS = {
    "Argentina": "Port of Buenos Aires, Argentina",
    "Bosnia": "Port of Ploče, Bosnia",
    "Brazil": "Port of Santos, Brazil",
    "Cambodia": "Port of Phnom Penh, Cambodia",
    "China": "Port of Shanghai, China",
    "Germany": "Port of Hamburg, Germany",
    "India": "Port of Mumbai, India",
    "Indonesia": "Port of Tanjung Priok, Jakarta, Indonesia",
    "Italy": "Port of Genoa, Italy",
    "Japan": "Port of Yokohama, Japan",
    "Myanmar": "Port of Yangon, Myanmar",
    "South Korea": "Port of Busan, South Korea",
    "Sri Lanka": "Port of Colombo, Sri Lanka",
    "Taiwan": "Port of Kaohsiung, Taiwan",
    "Thailand": "Port of Laem Chabang, Thailand",
    "Turkey": "Port of Istanbul, Turkey",
    "Vietnam": "Port of Ho Chi Minh, Vietnam"
}
MARITIME_TRANSPORT = {
    "Argentina": (0, 0),
    "Bosnia": (480, 600),
    "Brazil": (0, 0),
    "Cambodia": (720, 960),
    "China": (720, 960),
    "Germany": (480, 600),
    "India": (720, 960),
    "Indonesia": (840, 1080),
    "Italy": (480, 600),
    "Japan": (720, 960),
    "Myanmar": (720, 960),
    "South Korea": (720, 960),
    "Sri Lanka": (720, 960),
    "Taiwan": (720, 960),
    "Thailand": (720, 960),
    "Turkey": (720, 960),
    "Vietnam": (840, 1080)
}
ECONOMIC_HUBS = {
    "São Paulo": "São Paulo, SP",
    "Rio de Janeiro": "Rio de Janeiro, RJ",
    "Belo Horizonte": "Belo Horizonte, MG",
    "Porto Alegre": "Porto Alegre, RS",
    "Salvador": "Salvador, BA",
    "Recife": "Recife, PE",
    "Fortaleza": "Fortaleza, CE",
    "Curitiba": "Curitiba, PR",
    "Florianópolis": "Florianópolis, SC",
    "Goiânia": "Goiânia, GO"
}

def load_spreadsheet(url):
    """Load the spreadsheet from the given URL."""
    return pd.read_excel(url)

def initialize_gmaps(api_key):
    """Initialize the Google Maps client."""
    return googlemaps.Client(key=api_key)

def create_routes(spreadsheet_routes, major_ports):
    """Create routes based on the given spreadsheet data and major ports."""
    routes = []
    for city, country, zip_code, workers_count in spreadsheet_routes.values:
        port = major_ports.get(country)
        origin = f"{city}, {country}, {zip_code}"
        routes.append((origin, port, country, workers_count))
    return routes

def calculate_distance(gmaps, origin, destination):
    """Calculate the driving distance between origin and destination."""
    try:
        result = gmaps.directions(origin, destination, mode="driving", departure_time=datetime.now())
        if result and result[0]['legs']:
            duration = result[0]['legs'][0]['duration']['value'] / 3600  # Convert seconds to hours
            return duration
        else:
            print(f"Route not found or no valid legs from {origin} to {destination}")
            return None
    except Exception as e:
        print(f"Error trying to find route from {origin} to {destination}: {e}")
        return None

def calculate_terrestrial_times_origin(routes, gmaps, brand):
    """Calculate the terrestrial times from origin to port."""
    terrestrial_times_origin = {}
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for origin, destination, country, workers_count in routes:
            if country in ["Brazil", "Argentina"]:
                for hub, local_hub in ECONOMIC_HUBS.items():
                    futures.append((origin, local_hub, country, workers_count, executor.submit(calculate_distance, gmaps, origin, local_hub)))
            elif country == "South Korea":
                terrestrial_times_origin[(origin, destination, country, workers_count)] = 0
            else:
                futures.append((origin, destination, country, workers_count, executor.submit(calculate_distance, gmaps, origin, destination)))

        for future in tqdm(concurrent.futures.as_completed([f[4] for f in futures]), total=len(futures), desc=f"Calculating terrestrial origin times for {brand}"):
            for f in futures:
                if future == f[4]:
                    origin, destination, country, workers_count = f[0], f[1], f[2], f[3]
                    result = future.result()
                    terrestrial_times_origin[(origin, destination, country, workers_count)] = result if result is not None else None
    return terrestrial_times_origin

def calculate_total_times(routes, terrestrial_times, port_to_hub_distances, maritime_transport, brand, total_workers):
    """Calculate the total travel times including maritime and terrestrial transport."""
    total_times = []
    for (origin, destination, country, workers_count), time_origin_to_port in tqdm(terrestrial_times.items(), desc=f"Calculating total times for {brand}"):
        if time_origin_to_port is None:
            continue
        if country in ["Brazil", "Argentina"]:
            total_times.append({
                "Brand": brand,
                "Origin": origin,
                "Destination": destination,
                "Min Time": time_origin_to_port,
                "Max Time": time_origin_to_port,
                "Average Time": time_origin_to_port,
                "Workers Count": workers_count,
                "Adjusted Average Time": (time_origin_to_port * workers_count) / (total_workers * 10)
            })
        else:
            maritime_time_min, maritime_time_max = maritime_transport[country]
            for local_hub, duration_port_to_hub in port_to_hub_distances.items():
                if duration_port_to_hub is None:
                    continue
                total_min = time_origin_to_port + maritime_time_min + duration_port_to_hub
                total_max = time_origin_to_port + maritime_time_max + duration_port_to_hub
                total_avg = (total_min + total_max) / 2
                total_times.append({
                    "Brand": brand,
                    "Origin": origin,
                    "Destination": local_hub,
                    "Min Time": total_min,
                    "Max Time": total_max,
                    "Average Time": total_avg,
                    "Workers Count": workers_count,
                    "Adjusted Average Time": (total_avg * workers_count) / (total_workers * 10)
                })
    return total_times

def main():
    """Main function to execute the workflow."""
    df_spreadsheet = load_spreadsheet(GITHUB_URL)
    gmaps = initialize_gmaps(API_KEY_GOOGLE_MAPS)

    # Extract cities, countries, zip code, and worker counts from the spreadsheet
    nike_routes_spreadsheet = df_spreadsheet[df_spreadsheet['Empresa'] == 'Nike'][['City', 'Country / Region', 'Zip Code', 'Workers Count']]
    adidas_routes_spreadsheet = df_spreadsheet[df_spreadsheet['Empresa'] == 'Adidas'][['City', 'Country / Region', 'Zip Code', 'Workers Count']]
    vulcabras_routes_spreadsheet = df_spreadsheet[df_spreadsheet['Empresa'] == 'Vulcabras'][['City', 'Country / Region', 'Zip Code', 'Workers Count']]

    # Calculate total workers for each company
    total_workers_nike = nike_routes_spreadsheet['Workers Count'].sum()
    total_workers_adidas = adidas_routes_spreadsheet['Workers Count'].sum()
    total_workers_vulcabras = vulcabras_routes_spreadsheet['Workers Count'].sum()

    # Create routes
    nike_routes_new = create_routes(nike_routes_spreadsheet, MAJOR_PORTS)
    adidas_routes_new = create_routes(adidas_routes_spreadsheet, MAJOR_PORTS)
    vulcabras_routes_new = create_routes(vulcabras_routes_spreadsheet, MAJOR_PORTS)

    # Calculate distances from Port of Santos to each economic hub
    port_to_hub_distances = {}
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {executor.submit(calculate_distance, gmaps, "Port of Santos, SP", local_hub): local_hub for local_hub in ECONOMIC_HUBS.values()}
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="Calculating distances to economic hubs"):
            local_hub = futures[future]
            port_to_hub_distances[local_hub] = future.result()

    # Calculate terrestrial and total travel times for Nike, Adidas, and Vulcabras
    nike_terrestrial_times_origin = calculate_terrestrial_times_origin(nike_routes_new, gmaps, "Nike")
    nike_total_times = calculate_total_times(nike_routes_new, nike_terrestrial_times_origin, port_to_hub_distances, MARITIME_TRANSPORT, "Nike", total_workers_nike)

    adidas_terrestrial_times_origin = calculate_terrestrial_times_origin(adidas_routes_new, gmaps, "Adidas")
    adidas_total_times = calculate_total_times(adidas_routes_new, adidas_terrestrial_times_origin, port_to_hub_distances, MARITIME_TRANSPORT, "Adidas", total_workers_adidas)

    vulcabras_terrestrial_times_origin = calculate_terrestrial_times_origin(vulcabras_routes_new, gmaps, "Vulcabras")
    vulcabras_total_times = calculate_total_times(vulcabras_routes_new, vulcabras_terrestrial_times_origin, port_to_hub_distances, MARITIME_TRANSPORT, "Vulcabras", total_workers_vulcabras)

    # Organize results into DataFrames and save them
    df_nike_times = pd.DataFrame(nike_total_times)
    df_adidas_times = pd.DataFrame(adidas_total_times)
    df_vulcabras_times = pd.DataFrame(vulcabras_total_times)
    df_total_times = pd.concat([df_nike_times, df_adidas_times, df_vulcabras_times], axis=0, ignore_index=True)

    df_total_times.to_excel("estimated_delivery_times_unified.xlsx", index=False)
    print("Excel file saved successfully.")

    # Calculate adjusted average delivery time for Nike, Adidas, and Vulcabras
    nike_avg_time = df_nike_times['Adjusted Average Time'].sum()
    print(f"Adjusted average delivery time for Nike: {nike_avg_time:.2f} hours")

    adidas_avg_time = df_adidas_times['Adjusted Average Time'].sum()
    print(f"Adjusted average delivery time for Adidas: {adidas_avg_time:.2f} hours")

    vulcabras_avg_time = df_vulcabras_times['Adjusted Average Time'].sum()
    print(f"Adjusted average delivery time for Vulcabras: {vulcabras_avg_time:.2f} hours")

if __name__ == "__main__":
    main()
