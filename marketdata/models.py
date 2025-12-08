from django.db import models

# Create your models here.
class Sectors(models.Model):
    name = models.CharField(max_length=100, unique=True)

    def __str__(self):
        return self.name
    
    class Meta:
        verbose_name = "Sector"
        verbose_name_plural = "Sectors"


def validate_market_type(value):
    if value not in ['NSE', 'BSE']:
        raise ValidationError('Invalid market type')


class Symbol(models.Model):
    symbol = models.CharField(max_length=20, unique=True) 
    company_name = models.CharField(max_length=255, null=True, blank=True)
    sector = models.ForeignKey(Sectors, on_delete=models.SET_NULL, null=True)
    market_type = models.CharField(max_length=20, null=True, blank=True, validators=[validate_market_type])

    def __str__(self):
        return self.symbol
    
    class Meta:
        verbose_name = "Symbol"
        verbose_name_plural = "Symbols"

class EodPrice(models.Model):
    trade_date = models.DateField()
    symbol = models.ForeignKey(Symbol, on_delete=models.CASCADE)
    open = models.DecimalField(max_digits=12, decimal_places=2)
    high = models.DecimalField(max_digits=12, decimal_places=2)
    low = models.DecimalField(max_digits=12, decimal_places=2)
    close = models.DecimalField(max_digits=12, decimal_places=2)
    volume = models.BigIntegerField()
    market_cap = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)

    class Meta:
        unique_together = ("trade_date", "symbol")
        verbose_name = "EOD Price"
        verbose_name_plural = "EOD Prices"

    def __str__(self):
        return f"{self.symbol} - {self.trade_date}"


class Parameter(models.Model):
    trade_date = models.DateField()
    symbol = models.ForeignKey(Symbol, on_delete=models.CASCADE)

    closing_price = models.DecimalField(max_digits=12, decimal_places=2)
    rsc30 = models.DecimalField(max_digits=12, decimal_places=4, null=True)
    rsc500 = models.DecimalField(max_digits=12, decimal_places=4, null=True)

    ema21 = models.DecimalField(max_digits=12, decimal_places=4, null=True)
    ema50 = models.DecimalField(max_digits=12, decimal_places=4, null=True)
    ema200 = models.DecimalField(max_digits=12, decimal_places=4, null=True)

    class Meta:
        unique_together = ("trade_date", "symbol")
        verbose_name = "Parameter"
        verbose_name_plural = "Parameters"

    def __str__(self):
        return f"{self.symbol} - {self.trade_date}"