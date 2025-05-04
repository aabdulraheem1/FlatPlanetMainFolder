from . models import MasterDataProductModel

# Check if the product exists
product = MasterDataProductModel.objects.filter(Product='BK58105A').first()
if product:
    print(f"Product: {product.Product}, DressMass: {product.DressMass}")
else:
    print("Product BK58105A does not exist in MasterDataProductModel.")