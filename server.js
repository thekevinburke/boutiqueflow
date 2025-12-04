const express = require('express');
const multer = require('multer');
const Anthropic = require('@anthropic-ai/sdk').default;
const path = require('path');
require('dotenv').config();

const app = express();
const upload = multer({ storage: multer.memoryStorage() });

const anthropic = new Anthropic({
  apiKey: process.env.ANTHROPIC_API_KEY,
});

// Heartland API configuration
const HEARTLAND_BASE_URL = `https://${process.env.HEARTLAND_SUBDOMAIN}.retail.heartland.us/api`;
const HEARTLAND_TOKEN = process.env.HEARTLAND_API_TOKEN;

// Helper function to make Heartland API requests
async function heartlandRequest(endpoint, options = {}) {
  const url = `${HEARTLAND_BASE_URL}${endpoint}`;
  const response = await fetch(url, {
    ...options,
    headers: {
      'Authorization': `Bearer ${HEARTLAND_TOKEN}`,
      'Content-Type': 'application/json',
      ...options.headers,
    },
  });
  
  if (!response.ok) {
    throw new Error(`Heartland API error: ${response.status} ${response.statusText}`);
  }
  
  return response.json();
}

app.use(express.json());
app.use(express.static('public'));

// Generate product description from images
app.post('/api/generate', upload.array('images', 10), async (req, res) => {
  try {
    const { productName, vendor, color, category, vendorDescription } = req.body;
    const images = req.files;

    if (!images || images.length === 0) {
      return res.status(400).json({ error: 'At least one image is required' });
    }

    // Prepare image content for Claude
    const imageContent = images.map(img => ({
      type: 'image',
      source: {
        type: 'base64',
        media_type: img.mimetype,
        data: img.buffer.toString('base64'),
      },
    }));

    // Build the prompt
    const prompt = `You are a copywriter for Monkee's of Chattanooga, an upscale women's boutique known for stylish, curated fashion. Write a product description for our Shopify store.

Product Details:
- Product Name: ${productName || 'Not provided'}
- Vendor/Brand: ${vendor || 'Not provided'}
- Color: ${color || 'Not provided'}
- Category: ${category || 'Not provided'}
${vendorDescription ? `- Vendor's Description: ${vendorDescription}` : ''}

Based on the product images and details above, write a product description following this exact structure:

**OPENING (2-3 sentences):**
Start with an engaging hook that captures the item's appeal and vibe. Mention the brand name and product name naturally. Describe what makes this piece special and when/where to wear it.

**FEATURES (5-6 bullet points, one per line):**
Each bullet should be on its own line and cover:
- Key design details and embellishments visible in the images
- Fabric/material if known or visible
- Fit and silhouette (relaxed, fitted, oversized, etc.)
- Color and any accent colors or patterns
- Styling suggestions (what to pair it with)
- Care instructions if mentioned in vendor description, otherwise note "See label for care instructions"

Keep the tone warm, sophisticated, and aspirational—like a trusted friend who works in fashion giving advice. Avoid over-the-top phrases like "isn't just a [item]—it's a statement" or "elevate your wardrobe." Be genuine and specific.

Total length should be 150-250 words.

**META DESCRIPTION:**
Write an SEO-friendly meta description under 160 characters. Include the brand name, product type, and one key appeal (like the occasion, season, or standout feature).

Format your response exactly like this:
---DESCRIPTION---
[Opening sentences]

[Bullet points, each on its own line starting with •]

---META---
[Meta description]

---END---`;

    const response = await anthropic.messages.create({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 1024,
      messages: [
        {
          role: 'user',
          content: [
            ...imageContent,
            { type: 'text', text: prompt },
          ],
        },
      ],
    });

    const responseText = response.content[0].text;

    // Parse the response
    const descriptionMatch = responseText.match(/---DESCRIPTION---([\s\S]*?)---META---/);
    const metaMatch = responseText.match(/---META---([\s\S]*?)---END---/);

    const descriptionBase = descriptionMatch ? descriptionMatch[1].trim() : responseText;
    const description = descriptionBase + '\n\n**Not sure of the fit? Need more information?**\n**We\'re here to help! Send us a DM @monkeesofchattanooga or call 423-486-1300!**';
    const metaDescription = metaMatch ? metaMatch[1].trim() : '';

    res.json({
      success: true,
      description,
      metaDescription,
      raw: responseText,
    });

  } catch (error) {
    console.error('Error generating description:', error);
    res.status(500).json({ error: error.message });
  }
});

// Cache for vendor names (to avoid repeated API calls)
const vendorCache = new Map();

async function getVendorName(vendorId) {
  if (!vendorId) return 'Unknown Vendor';
  
  if (vendorCache.has(vendorId)) {
    return vendorCache.get(vendorId);
  }
  
  try {
    const vendor = await heartlandRequest(`/purchasing/vendors/${vendorId}`);
    const name = vendor.name || 'Unknown Vendor';
    vendorCache.set(vendorId, name);
    return name;
  } catch (error) {
    console.error(`Error fetching vendor ${vendorId}:`, error);
    return 'Unknown Vendor';
  }
}

// Get all receipts from Heartland
app.get('/api/receipts', async (req, res) => {
  try {
    // Fetch recent receipts (last 30 days, limit 50), sorted by updated_at descending (newest first)
    const thirtyDaysAgo = new Date();
    thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);
    const dateFilter = thirtyDaysAgo.toISOString().split('T')[0];
    
    const data = await heartlandRequest(`/purchasing/receipts?per_page=50&_filter[created_at][$gte]=${dateFilter}&sort[]=updated_at,desc`);
    
    // Process receipts sequentially to avoid race conditions
    const receipts = [];
    for (const r of data.results) {
      let vendorName = 'Unknown Vendor';
      let itemCount = 0;
      
      // Get lines and extract vendor from first item
      try {
        const lines = await heartlandRequest(`/purchasing/receipts/${r.id}/lines?per_page=10`);
        itemCount = lines.total || 0;
        
        // Get vendor from first item's primary_vendor_id
        if (lines.results && lines.results.length > 0) {
          const firstLine = lines.results[0];
          if (firstLine.item_id) {
            try {
              const item = await heartlandRequest(`/items/${firstLine.item_id}`);
              if (item.primary_vendor_id) {
                vendorName = await getVendorName(item.primary_vendor_id);
              }
            } catch (e) {
              console.error('Error fetching item for vendor:', e);
            }
          }
        }
      } catch (e) {
        console.error('Error fetching receipt lines:', e);
      }
      
      receipts.push({
        id: `REC-${r.id}`,
        heartlandId: r.id,
        date: r.updated_at ? r.updated_at.split('T')[0] : (r.created_at ? r.created_at.split('T')[0] : new Date().toISOString().split('T')[0]),
        vendor: vendorName,
        poNumber: r.public_id || `REC-${r.id}`,
        itemCount: itemCount,
        status: r.status === 'complete' ? 'completed' : r.status === 'pending' ? 'new' : 'in_progress',
      });
    }
    
    res.json(receipts);
  } catch (error) {
    console.error('Error fetching receipts from Heartland:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get single receipt with items
app.get('/api/receipts/:id', async (req, res) => {
  try {
    // Extract Heartland ID from our ID format (REC-123 -> 123)
    const heartlandId = req.params.id.replace('REC-', '');
    
    const receipt = await heartlandRequest(`/purchasing/receipts/${heartlandId}`);
    const linesData = await heartlandRequest(`/purchasing/receipts/${heartlandId}/lines?per_page=100`);
    
    let vendorName = 'Unknown Vendor';
    
    // Fetch item details for each line
    const items = await Promise.all(linesData.results.map(async (line) => {
      let itemDetails = {
        description: 'Unknown Item',
        custom: {}
      };
      
      try {
        itemDetails = await heartlandRequest(`/items/${line.item_id}`);
        
        // Get vendor from first item's primary_vendor_id (set once for the receipt)
        if (vendorName === 'Unknown Vendor' && itemDetails.primary_vendor_id) {
          vendorName = await getVendorName(itemDetails.primary_vendor_id);
        }
      } catch (e) {
        console.error(`Error fetching item ${line.item_id}:`, e);
      }
      
      return {
        id: `ITEM-${line.id}`,
        heartlandItemId: line.item_id,
        heartlandLineId: line.id,
        name: itemDetails.description || 'Unknown Item',
        color: itemDetails.custom?.color || itemDetails.custom?.Color || '',
        size: itemDetails.custom?.size || itemDetails.custom?.Size || '',
        category: itemDetails.custom?.category || itemDetails.custom?.Category || itemDetails.custom?.department || '',
        status: 'new', // We'll track this separately later
        qty: line.qty,
        unitCost: line.unit_cost,
      };
    }));
    
    res.json({
      id: req.params.id,
      heartlandId: receipt.id,
      date: receipt.created_at ? receipt.created_at.split('T')[0] : new Date().toISOString().split('T')[0],
      vendor: vendorName,
      poNumber: receipt.public_id || `REC-${receipt.id}`,
      itemCount: items.length,
      status: receipt.status === 'complete' ? 'completed' : receipt.status === 'pending' ? 'new' : 'in_progress',
      items: items,
    });
  } catch (error) {
    console.error('Error fetching receipt from Heartland:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get single item
app.get('/api/items/:id', async (req, res) => {
  try {
    // Extract Heartland line ID from our ID format (ITEM-123 -> 123)
    const lineId = req.params.id.replace('ITEM-', '');
    
    // We need to find which receipt this line belongs to
    // For now, we'll need the item_id to be passed or we search
    // Let's try to get it from query param or search
    
    const itemId = req.query.itemId;
    
    if (itemId) {
      const item = await heartlandRequest(`/items/${itemId}`);
      
      // Try to get vendor from primary_vendor_id
      let vendorName = 'Unknown Vendor';
      if (item.primary_vendor_id) {
        vendorName = await getVendorName(item.primary_vendor_id);
      }
      
      res.json({
        id: req.params.id,
        heartlandItemId: itemId,
        name: item.description || 'Unknown Item',
        color: item.custom?.color || item.custom?.Color || '',
        size: item.custom?.size || item.custom?.Size || '',
        category: item.custom?.category || item.custom?.Category || item.custom?.department || '',
        vendor: vendorName,
        status: 'new',
        longDescription: item.long_description || '',
        price: item.price,
        cost: item.cost,
      });
    } else {
      // If no itemId provided, return error
      res.status(400).json({ error: 'itemId query parameter required' });
    }
  } catch (error) {
    console.error('Error fetching item from Heartland:', error);
    res.status(500).json({ error: error.message });
  }
});

// Update item description in Heartland
app.put('/api/items/:id', async (req, res) => {
  try {
    const itemId = req.query.itemId || req.params.id.replace('ITEM-', '');
    const { longDescription } = req.body;
    
    await heartlandRequest(`/items/${itemId}`, {
      method: 'PUT',
      body: JSON.stringify({
        long_description: longDescription,
      }),
    });
    
    res.json({ success: true });
  } catch (error) {
    console.error('Error updating item in Heartland:', error);
    res.status(500).json({ error: error.message });
  }
});

// Health check endpoint
app.get('/api/health', async (req, res) => {
  try {
    // Test Heartland connection
    await heartlandRequest('/system/whoami');
    res.json({ 
      status: 'ok',
      heartland: 'connected',
      anthropic: process.env.ANTHROPIC_API_KEY ? 'configured' : 'missing',
    });
  } catch (error) {
    res.json({ 
      status: 'degraded',
      heartland: 'error: ' + error.message,
      anthropic: process.env.ANTHROPIC_API_KEY ? 'configured' : 'missing',
    });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`BoutiqueFlow running at http://localhost:${PORT}`);
  console.log(`Heartland API: ${HEARTLAND_BASE_URL}`);
});
