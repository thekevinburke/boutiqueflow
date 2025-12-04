const express = require('express');
const multer = require('multer');
const Anthropic = require('@anthropic-ai/sdk').default;
const path = require('path');
const { Pool } = require('pg');
require('dotenv').config();

const app = express();
const upload = multer({ storage: multer.memoryStorage() });

// Database connection
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes('railway') ? { rejectUnauthorized: false } : false,
});

// Initialize database tables
async function initDatabase() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS processed_items (
        id SERIAL PRIMARY KEY,
        item_type VARCHAR(10) NOT NULL,
        heartland_id VARCHAR(50) NOT NULL,
        receipt_id VARCHAR(50),
        status VARCHAR(20) NOT NULL DEFAULT 'new',
        processed_at TIMESTAMP,
        processed_by VARCHAR(100),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(item_type, heartland_id)
      )
    `);
    console.log('Database initialized successfully');
  } catch (error) {
    console.error('Error initializing database:', error);
  }
}

// Initialize DB on startup
initDatabase();

// Basic Auth middleware
const USERS = {
  'adminkevin': 'Monkees842',
  // Add more users here as needed, e.g.:
  // 'testuser': 'testpassword',
};

function basicAuth(req, res, next) {
  // Skip auth for health check endpoint
  if (req.path === '/api/health') {
    return next();
  }

  const authHeader = req.headers.authorization;

  if (!authHeader || !authHeader.startsWith('Basic ')) {
    res.setHeader('WWW-Authenticate', 'Basic realm="BoutiqueFlow"');
    return res.status(401).send('Authentication required');
  }

  const base64Credentials = authHeader.split(' ')[1];
  const credentials = Buffer.from(base64Credentials, 'base64').toString('ascii');
  const [username, password] = credentials.split(':');

  if (USERS[username] && USERS[username] === password) {
    req.username = username; // Store username for tracking who processed items
    return next();
  }

  res.setHeader('WWW-Authenticate', 'Basic realm="BoutiqueFlow"');
  return res.status(401).send('Invalid credentials');
}

// Apply Basic Auth to all routes
app.use(basicAuth);

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
    // Try to get more details from the response
    let errorDetails = '';
    try {
      const errorBody = await response.text();
      errorDetails = ` - ${errorBody}`;
      console.error('Heartland API error details:', errorBody);
    } catch (e) {
      // Ignore if we can't read the body
    }
    throw new Error(`Heartland API error: ${response.status} ${response.statusText}${errorDetails}`);
  }
  
  return response.json();
}

// Helper function to get item status from database
async function getItemStatus(itemType, heartlandId) {
  try {
    const result = await pool.query(
      'SELECT status FROM processed_items WHERE item_type = $1 AND heartland_id = $2',
      [itemType, heartlandId.toString()]
    );
    return result.rows[0]?.status || 'new';
  } catch (error) {
    console.error('Error getting item status:', error);
    return 'new';
  }
}

// Helper function to update item status in database
async function updateItemStatus(itemType, heartlandId, status, receiptId = null, username = null) {
  try {
    await pool.query(`
      INSERT INTO processed_items (item_type, heartland_id, receipt_id, status, processed_at, processed_by)
      VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP, $5)
      ON CONFLICT (item_type, heartland_id)
      DO UPDATE SET status = $4, processed_at = CURRENT_TIMESTAMP, processed_by = $5, updated_at = CURRENT_TIMESTAMP
    `, [itemType, heartlandId.toString(), receiptId, status, username]);
    return true;
  } catch (error) {
    console.error('Error updating item status:', error);
    return false;
  }
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
    // Fetch recent COMPLETE receipts (last 30 days, limit 50), sorted by updated_at descending (newest first)
    const thirtyDaysAgo = new Date();
    thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);
    const dateFilter = thirtyDaysAgo.toISOString().split('T')[0];
    
    const data = await heartlandRequest(`/purchasing/receipts?per_page=50&_filter[created_at][$gte]=${dateFilter}&_filter[status]=complete&sort[]=updated_at,desc`);
    
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
        receiptNumber: r.public_id || `${r.id}`,
        itemCount: itemCount,
        status: 'new', // Receipt-level status (could be computed from items later)
      });
    }
    
    res.json(receipts);
  } catch (error) {
    console.error('Error fetching receipts from Heartland:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get single receipt with items grouped by grid
app.get('/api/receipts/:id', async (req, res) => {
  try {
    // Extract Heartland ID from our ID format (REC-123 -> 123)
    const heartlandId = req.params.id.replace('REC-', '');
    
    const receipt = await heartlandRequest(`/purchasing/receipts/${heartlandId}`);
    const linesData = await heartlandRequest(`/purchasing/receipts/${heartlandId}/lines?per_page=100`);
    
    let vendorName = 'Unknown Vendor';
    
    // Fetch item details for each line
    const rawItems = [];
    for (const line of linesData.results) {
      let itemDetails = {
        description: 'Unknown Item',
        custom: {},
        grid_id: null
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
      
      rawItems.push({
        heartlandItemId: line.item_id,
        heartlandLineId: line.id,
        name: itemDetails.description || 'Unknown Item',
        colorName: itemDetails.custom?.color_name || itemDetails.custom?.Color_Name || itemDetails.custom?.color || itemDetails.custom?.Color || '',
        size: itemDetails.custom?.size || itemDetails.custom?.Size || '',
        category: itemDetails.custom?.category || itemDetails.custom?.Category || itemDetails.custom?.department || '',
        styleName: itemDetails.custom?.style_name || itemDetails.custom?.Style_Name || '',
        gridId: itemDetails.grid_id || null,
        longDescription: itemDetails.long_description || '',
        qty: line.qty,
        unitCost: line.unit_cost,
      });
    }
    
    // Group items by grid_id (null grid_id = standalone item)
    const gridGroups = new Map();
    const standaloneItems = [];
    
    for (const item of rawItems) {
      if (item.gridId) {
        if (!gridGroups.has(item.gridId)) {
          gridGroups.set(item.gridId, {
            gridId: item.gridId,
            styleName: item.styleName || item.name.split(' - ')[0], // Use style name or first part of description
            category: item.category,
            longDescription: item.longDescription,
            variants: [],
            colors: new Set(),
            sizes: new Set(),
          });
        }
        const group = gridGroups.get(item.gridId);
        group.variants.push(item);
        if (item.colorName) group.colors.add(item.colorName);
        if (item.size) group.sizes.add(item.size);
      } else {
        standaloneItems.push(item);
      }
    }
    
    // Convert grid groups to array format for frontend
    const items = [];
    
    // Add grid groups
    for (const [gridId, group] of gridGroups) {
      // Get status from database
      const status = await getItemStatus('grid', gridId);
      
      items.push({
        id: `GRID-${gridId}`,
        type: 'grid',
        gridId: gridId,
        name: group.styleName,
        category: group.category,
        colors: Array.from(group.colors).sort(),
        sizes: Array.from(group.sizes).sort(),
        variantCount: group.variants.length,
        longDescription: group.longDescription,
        status: status,
        // Include first variant's item ID for fetching additional details if needed
        heartlandItemId: group.variants[0]?.heartlandItemId,
      });
    }
    
    // Add standalone items
    for (const item of standaloneItems) {
      // Get status from database
      const status = await getItemStatus('item', item.heartlandItemId);
      
      items.push({
        id: `ITEM-${item.heartlandLineId}`,
        type: 'item',
        heartlandItemId: item.heartlandItemId,
        name: item.name,
        category: item.category,
        colors: item.colorName ? [item.colorName] : [],
        sizes: item.size ? [item.size] : [],
        variantCount: 1,
        longDescription: item.longDescription,
        status: status,
      });
    }
    
    res.json({
      id: req.params.id,
      heartlandId: receipt.id,
      date: receipt.created_at ? receipt.created_at.split('T')[0] : new Date().toISOString().split('T')[0],
      vendor: vendorName,
      receiptNumber: receipt.public_id || `${receipt.id}`,
      itemCount: rawItems.length,
      productCount: items.length, // Number of unique products (grids + standalone)
      status: 'new',
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
    
    const itemId = req.query.itemId;
    
    if (itemId) {
      const item = await heartlandRequest(`/items/${itemId}`);
      
      // Try to get vendor from primary_vendor_id
      let vendorName = 'Unknown Vendor';
      if (item.primary_vendor_id) {
        vendorName = await getVendorName(item.primary_vendor_id);
      }
      
      // Get status from database
      const status = await getItemStatus('item', itemId);
      
      res.json({
        id: req.params.id,
        type: 'item',
        heartlandItemId: itemId,
        name: item.description || 'Unknown Item',
        colors: [item.custom?.color_name || item.custom?.Color_Name || item.custom?.color || item.custom?.Color || ''].filter(c => c),
        sizes: [item.custom?.size || item.custom?.Size || ''].filter(s => s),
        category: item.custom?.category || item.custom?.Category || item.custom?.department || '',
        vendor: vendorName,
        status: status,
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

// Get single grid
app.get('/api/grids/:id', async (req, res) => {
  try {
    const gridId = req.params.id.replace('GRID-', '');
    
    const grid = await heartlandRequest(`/item_grids/${gridId}`);
    
    // Get vendor from grid's primary_vendor_id
    let vendorName = 'Unknown Vendor';
    if (grid.item_primary_vendor_id) {
      vendorName = await getVendorName(grid.item_primary_vendor_id);
    }
    
    // Get items in this grid to show color/size info and get style_name/category
    const itemsData = await heartlandRequest(`/items?_filter[grid_id]=${gridId}&per_page=50`);
    
    const colors = new Set();
    const sizes = new Set();
    let styleName = '';
    let category = '';
    
    for (const item of itemsData.results || []) {
      const colorName = item.custom?.color_name || item.custom?.Color_Name || item.custom?.color || item.custom?.Color || '';
      const size = item.custom?.size || item.custom?.Size || '';
      if (colorName) colors.add(colorName);
      if (size) sizes.add(size);
      
      // Get style_name and category from first item that has them
      if (!styleName) {
        styleName = item.custom?.style_name || item.custom?.Style_Name || '';
      }
      if (!category) {
        category = item.custom?.category || item.custom?.Category || item.custom?.department || item.custom?.Department || '';
      }
    }
    
    // Get status from database
    const status = await getItemStatus('grid', gridId);
    
    // Use item_description from grid, fall back to style_name from items, then 'Unknown Grid'
    const gridName = grid.item_description || styleName || 'Unknown Grid';
    
    res.json({
      id: `GRID-${gridId}`,
      type: 'grid',
      gridId: gridId,
      name: gridName,
      colors: Array.from(colors).sort(),
      sizes: Array.from(sizes).sort(),
      variantCount: itemsData.total || 0,
      category: grid.custom?.category || grid.custom?.Category || category || '',
      vendor: vendorName,
      status: status,
      longDescription: grid.item_long_description || '',
      price: grid.item_price,
      cost: grid.item_cost,
    });
  } catch (error) {
    console.error('Error fetching grid from Heartland:', error);
    res.status(500).json({ error: error.message });
  }
});

// Update item description in Heartland and mark as completed
app.put('/api/items/:id', async (req, res) => {
  try {
    const itemId = req.query.itemId || req.params.id.replace('ITEM-', '');
    const { longDescription, status } = req.body;
    
    // If there's a description, update Heartland
    if (longDescription !== undefined) {
      await heartlandRequest(`/items/${itemId}`, {
        method: 'PUT',
        body: JSON.stringify({
          long_description: longDescription,
        }),
      });
    }
    
    // Update status in database
    const newStatus = status || 'completed';
    await updateItemStatus('item', itemId, newStatus, null, req.username);
    
    res.json({ success: true, status: newStatus });
  } catch (error) {
    console.error('Error updating item in Heartland:', error);
    res.status(500).json({ error: error.message });
  }
});

// Update grid description in Heartland and mark as completed
app.put('/api/grids/:id', async (req, res) => {
  try {
    const gridId = req.params.id.replace('GRID-', '');
    const { longDescription, status } = req.body;
    
    // If there's a description, update Heartland
    if (longDescription !== undefined) {
      await heartlandRequest(`/item_grids/${gridId}`, {
        method: 'PUT',
        body: JSON.stringify({
          item_long_description: longDescription,
        }),
      });
    }
    
    // Update status in database
    const newStatus = status || 'completed';
    await updateItemStatus('grid', gridId, newStatus, null, req.username);
    
    res.json({ success: true, status: newStatus });
  } catch (error) {
    console.error('Error updating grid in Heartland:', error);
    res.status(500).json({ error: error.message });
  }
});

// Skip an item (mark as skipped without updating Heartland)
app.post('/api/items/:id/skip', async (req, res) => {
  try {
    const itemId = req.query.itemId || req.params.id.replace('ITEM-', '');
    await updateItemStatus('item', itemId, 'skipped', null, req.username);
    res.json({ success: true, status: 'skipped' });
  } catch (error) {
    console.error('Error skipping item:', error);
    res.status(500).json({ error: error.message });
  }
});

// Skip a grid (mark as skipped without updating Heartland)
app.post('/api/grids/:id/skip', async (req, res) => {
  try {
    const gridId = req.params.id.replace('GRID-', '');
    await updateItemStatus('grid', gridId, 'skipped', null, req.username);
    res.json({ success: true, status: 'skipped' });
  } catch (error) {
    console.error('Error skipping grid:', error);
    res.status(500).json({ error: error.message });
  }
});

// Health check endpoint
app.get('/api/health', async (req, res) => {
  try {
    // Test Heartland connection
    await heartlandRequest('/system/whoami');
    
    // Test database connection
    await pool.query('SELECT 1');
    
    res.json({ 
      status: 'ok',
      heartland: 'connected',
      database: 'connected',
      anthropic: process.env.ANTHROPIC_API_KEY ? 'configured' : 'missing',
    });
  } catch (error) {
    res.json({ 
      status: 'degraded',
      heartland: 'error: ' + error.message,
      database: 'unknown',
      anthropic: process.env.ANTHROPIC_API_KEY ? 'configured' : 'missing',
    });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`BoutiqueFlow running at http://localhost:${PORT}`);
  console.log(`Heartland API: ${HEARTLAND_BASE_URL}`);
});
