import graphene
from graphene import ObjectType, String, Int, Float, List, Field, Boolean
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)

# Tipos GraphQL
class Customer(ObjectType):
    """Tipo GraphQL para Cliente"""
    customer_id = Int()
    business_name = String()
    name = String()
    email = String()
    vat_number = String()
    street_name = String()
    postal_code = Int()
    city = String()
    province_id = Int()
    country_id = String()
    phone = String()

class Product(ObjectType):
    """Tipo GraphQL para Producto"""
    product_id = String()
    reference = String()
    description = String()
    price = Float()
    stock = Int()
    active = Boolean()

class Invoice(ObjectType):
    """Tipo GraphQL para Factura"""
    invoice_id = Int()
    reference = String()
    customer_name = String()
    amount = Float()
    date = String()
    due_date = String()
    status = String()

class CacheStats(ObjectType):
    """Estadísticas del cache"""
    type = String()
    connected = Boolean()
    keys = Int()
    memory_usage = String()
    uptime = String()
    error = String()

# Queries
class Query(ObjectType):
    """Consultas GraphQL principales"""
    
    # Clientes
    customers = List(Customer, limit=Int(default_value=100), search=String())
    customer = Field(Customer, customer_id=Int(required=True))
    
    # Productos
    products = List(Product, limit=Int(default_value=100), search=String())
    product = Field(Product, product_id=String(required=True))
    
    # Facturas
    invoices = List(Invoice, limit=Int(default_value=50), from_date=String())
    invoice = Field(Invoice, invoice_id=Int(required=True))
    
    # Cache
    cache_stats = Field(CacheStats)
    
    def resolve_customers(self, info, limit=100, search=None):
        """Resolver para lista de clientes"""
        try:
            # Obtener instancias desde el contexto
            gomanage_client = info.context.get('gomanage_client')
            cache_manager = info.context.get('cache_manager')
            
            if not gomanage_client:
                logger.error("GoManage client no disponible")
                return []
            
            # Intentar obtener del cache primero
            cache_key = f"customers_{limit}_{search or 'all'}"
            cached_data = cache_manager.get(cache_key) if cache_manager else None
            
            if cached_data:
                logger.info(f"✅ Clientes obtenidos del cache: {len(cached_data)}")
                customers = cached_data
            else:
                # Obtener de GoManage
                logger.info("🔄 Obteniendo clientes de GoManage...")
                customers = gomanage_client.get_customers(limit=limit)
                
                # Guardar en cache por 1 hora
                if cache_manager and customers:
                    cache_manager.set(cache_key, customers, ttl=3600)
                    logger.info(f"💾 {len(customers)} clientes guardados en cache")
            
            # Filtrar por búsqueda si se especifica
            if search and customers:
                search_lower = search.lower()
                customers = [
                    c for c in customers 
                    if search_lower in (c.get('business_name', '') or '').lower() or
                       search_lower in (c.get('vat_number', '') or '').lower() or
                       search_lower in str(c.get('customer_id', '')).lower()
                ]
            
            return customers
            
        except Exception as e:
            logger.error(f"❌ Error obteniendo clientes: {e}")
            return []
    
    def resolve_customer(self, info, customer_id):
        """Resolver para cliente específico"""
        try:
            customers = self.resolve_customers(info, limit=1000)
            for customer in customers:
                if customer.get('customer_id') == customer_id:
                    return customer
            return None
        except Exception as e:
            logger.error(f"❌ Error obteniendo cliente {customer_id}: {e}")
            return None
    
    def resolve_products(self, info, limit=100, search=None):
        """Resolver para lista de productos"""
        try:
            gomanage_client = info.context.get('gomanage_client')
            cache_manager = info.context.get('cache_manager')
            
            if not gomanage_client:
                logger.error("GoManage client no disponible")
                return []
            
            # Intentar obtener del cache primero
            cache_key = f"products_{limit}_{search or 'all'}"
            cached_data = cache_manager.get(cache_key) if cache_manager else None
            
            if cached_data:
                logger.info(f"✅ Productos obtenidos del cache: {len(cached_data)}")
                products = cached_data
            else:
                # Obtener de GoManage
                logger.info("🔄 Obteniendo productos de GoManage...")
                products = gomanage_client.get_products(limit=limit)
                
                # Guardar en cache por 2 horas
                if cache_manager and products:
                    cache_manager.set(cache_key, products, ttl=7200)
                    logger.info(f"💾 {len(products)} productos guardados en cache")
            
            # Filtrar por búsqueda si se especifica
            if search and products:
                search_lower = search.lower()
                products = [
                    p for p in products 
                    if search_lower in (p.get('reference', '') or '').lower() or
                       search_lower in (p.get('description', '') or '').lower()
                ]
            
            return products
            
        except Exception as e:
            logger.error(f"❌ Error obteniendo productos: {e}")
            return []
    
    def resolve_product(self, info, product_id):
        """Resolver para producto específico"""
        try:
            products = self.resolve_products(info, limit=1000)
            for product in products:
                if product.get('product_id') == product_id:
                    return product
            return None
        except Exception as e:
            logger.error(f"❌ Error obteniendo producto {product_id}: {e}")
            return None
    
    def resolve_invoices(self, info, limit=50, from_date=None):
        """Resolver para lista de facturas"""
        try:
            gomanage_client = info.context.get('gomanage_client')
            cache_manager = info.context.get('cache_manager')
            
            if not gomanage_client:
                logger.error("GoManage client no disponible")
                return []
            
            # Cache más corto para facturas (30 minutos)
            cache_key = f"invoices_{limit}_{from_date or 'all'}"
            cached_data = cache_manager.get(cache_key) if cache_manager else None
            
            if cached_data:
                logger.info(f"✅ Facturas obtenidas del cache: {len(cached_data)}")
                return cached_data
            else:
                # Obtener de GoManage
                logger.info("🔄 Obteniendo facturas de GoManage...")
                invoices = gomanage_client.get_invoices(limit=limit, from_date=from_date)
                
                # Guardar en cache por 30 minutos
                if cache_manager and invoices:
                    cache_manager.set(cache_key, invoices, ttl=1800)
                    logger.info(f"💾 {len(invoices)} facturas guardadas en cache")
                
                return invoices
            
        except Exception as e:
            logger.error(f"❌ Error obteniendo facturas: {e}")
            return []
    
    def resolve_invoice(self, info, invoice_id):
        """Resolver para factura específica"""
        try:
            invoices = self.resolve_invoices(info, limit=1000)
            for invoice in invoices:
                if invoice.get('invoice_id') == invoice_id:
                    return invoice
            return None
        except Exception as e:
            logger.error(f"❌ Error obteniendo factura {invoice_id}: {e}")
            return None
    
    def resolve_cache_stats(self, info):
        """Resolver para estadísticas del cache"""
        try:
            cache_manager = info.context.get('cache_manager')
            if cache_manager:
                return cache_manager.get_stats()
            else:
                return {
                    "type": "None",
                    "connected": False,
                    "error": "Cache manager no disponible"
                }
        except Exception as e:
            logger.error(f"❌ Error obteniendo estadísticas del cache: {e}")
            return {
                "type": "Error",
                "connected": False,
                "error": str(e)
            }

# Mutaciones
class CreateCustomer(graphene.Mutation):
    """Mutación para crear cliente"""
    
    class Arguments:
        business_name = String(required=True)
        vat_number = String(required=True)
        name = String()
        email = String()
        street_name = String()
        postal_code = Int()
        city = String()
        province_id = Int()
        country_id = String()
        phone = String()
    
    customer = Field(Customer)
    success = Boolean()
    message = String()
    
    def mutate(self, info, business_name, vat_number, **kwargs):
        try:
            gomanage_client = info.context.get('gomanage_client')
            cache_manager = info.context.get('cache_manager')
            
            if not gomanage_client:
                return CreateCustomer(
                    success=False,
                    message="GoManage client no disponible"
                )
            
            # Preparar datos del cliente
            customer_data = {
                'business_name': business_name,
                'vat_number': vat_number,
                'name': kwargs.get('name', business_name),
                'email': kwargs.get('email', ''),
                'street_name': kwargs.get('street_name', ''),
                'postal_code': kwargs.get('postal_code'),
                'city': kwargs.get('city', ''),
                'province_id': kwargs.get('province_id'),
                'country_id': kwargs.get('country_id', 'ES'),
                'phone': kwargs.get('phone', '')
            }
            
            # Crear cliente en GoManage
            result = gomanage_client.create_customer(customer_data)
            
            if result:
                # Limpiar cache de clientes
                if cache_manager:
                    cache_manager.delete('customers_100_all')
                    cache_manager.delete('customers_1000_all')
                
                return CreateCustomer(
                    customer=customer_data,
                    success=True,
                    message="Cliente creado exitosamente"
                )
            else:
                return CreateCustomer(
                    success=False,
                    message="Error creando cliente en GoManage"
                )
                
        except Exception as e:
            logger.error(f"❌ Error en mutación crear cliente: {e}")
            return CreateCustomer(
                success=False,
                message=f"Error: {str(e)}"
            )

class Mutations(ObjectType):
    """Mutaciones disponibles"""
    create_customer = CreateCustomer.Field()

# Schema principal
schema = graphene.Schema(query=Query, mutation=Mutations)

