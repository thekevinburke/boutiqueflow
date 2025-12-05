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
    
    // Inventory analysis cache table
    await pool.query(`
      CREATE TABLE IF NOT EXISTS inventory_cache (
        id SERIAL PRIMARY KEY,
        cache_key VARCHAR(50) NOT NULL UNIQUE,
        data JSONB NOT NULL,
        synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);
    
    // Sales transactions table (foundation for SalesIQ, First Dibs, Retargeting)
    await pool.query(`
      CREATE TABLE IF NOT EXISTS sales_transactions (
        id SERIAL PRIMARY KEY,
        heartland_ticket_id VARCHAR(50) NOT NULL,
        heartland_line_id VARCHAR(50) DEFAULT '0',
        customer_id VARCHAR(50),
        item_id VARCHAR(50),
        transaction_date TIMESTAMP NOT NULL,
        day_of_week INTEGER,
        hour_of_day INTEGER,
        quantity INTEGER,
        unit_price DECIMAL(10,2),
        total_amount DECIMAL(10,2),
        category VARCHAR(100),
        vendor VARCHAR(100),
        brand VARCHAR(100),
        item_name VARCHAR(255),
        item_size VARCHAR(50),
        item_color VARCHAR(100),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(heartland_ticket_id, heartland_line_id)
      )
    `);
    
    // Create index for faster queries
    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_sales_customer ON sales_transactions(customer_id);
    `);
    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_sales_date ON sales_transactions(transaction_date);
    `);
    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_sales_item ON sales_transactions(item_id);
    `);
    
    // Customer profiles table (for First Dibs & Retargeting)
    await pool.query(`
      CREATE TABLE IF NOT EXISTS customers (
        id SERIAL PRIMARY KEY,
        heartland_customer_id VARCHAR(50) UNIQUE NOT NULL,
        first_name VARCHAR(100),
        last_name VARCHAR(100),
        email VARCHAR(255),
        phone VARCHAR(50),
        total_purchases INTEGER DEFAULT 0,
        lifetime_value DECIMAL(10,2) DEFAULT 0,
        first_purchase_date TIMESTAMP,
        last_purchase_date TIMESTAMP,
        avg_purchase_value DECIMAL(10,2),
        preferred_brands TEXT,
        preferred_sizes TEXT,
        preferred_categories TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);
    
    // Sales cache table (pre-aggregated data for SalesIQ)
    await pool.query(`
      CREATE TABLE IF NOT EXISTS sales_cache (
        id SERIAL PRIMARY KEY,
        cache_key VARCHAR(50) UNIQUE NOT NULL,
        data JSONB NOT NULL,
        synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);
    
    // Sync log for tracking/debugging
    await pool.query(`
      CREATE TABLE IF NOT EXISTS sync_log (
        id SERIAL PRIMARY KEY,
        sync_type VARCHAR(50) NOT NULL,
        started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        completed_at TIMESTAMP,
        status VARCHAR(20) DEFAULT 'running',
        records_processed INTEGER DEFAULT 0,
        error_message TEXT,
        duration_seconds INTEGER
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
  
  // Handle empty responses (common for PUT/DELETE)
  const text = await response.text();
  if (!text) {
    return { success: true };
  }
  
  try {
    return JSON.parse(text);
  } catch (e) {
    // If it's not JSON, return the text
    return { success: true, body: text };
  }
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
      DO UPDATE SET status = $4, receipt_id = $3, processed_at = CURRENT_TIMESTAMP, processed_by = $5, updated_at = CURRENT_TIMESTAMP
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
    
    // Convert to HTML format for Shopify
    const htmlDescription = convertToHtml(descriptionBase);
    
    const metaDescription = metaMatch ? metaMatch[1].trim() : '';

    res.json({
      success: true,
      description: htmlDescription,
      metaDescription,
      raw: responseText,
    });

  } catch (error) {
    console.error('Error generating description:', error);
    res.status(500).json({ error: error.message });
  }
});

// Convert plain text description to HTML for Shopify
function convertToHtml(text) {
  const lines = text.split('\n').filter(line => line.trim());
  let html = '';
  let inList = false;
  
  for (const line of lines) {
    const trimmed = line.trim();
    
    // Check if it's a bullet point
    if (trimmed.startsWith('•') || trimmed.startsWith('-') || trimmed.startsWith('*')) {
      if (!inList) {
        html += '<ul>\n';
        inList = true;
      }
      // Remove the bullet character and trim
      const content = trimmed.replace(/^[•\-\*]\s*/, '');
      html += `<li>${content}</li>\n`;
    } else {
      // It's a paragraph
      if (inList) {
        html += '</ul>\n';
        inList = false;
      }
      if (trimmed) {
        html += `<p>${trimmed}</p>\n`;
      }
    }
  }
  
  // Close list if still open
  if (inList) {
    html += '</ul>\n';
  }
  
  // Add the CTA footer
  html += '<p><strong>Not sure of the fit? Need more information?</strong></p>\n';
  html += '<p><strong>We\'re here to help! Send us a DM @monkeesofchattanooga or call 423-486-1300!</strong></p>';
  
  return html;
}

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
    
    // Get all receipt IDs to batch query statuses
    const receiptIds = data.results.map(r => r.id.toString());
    
    // Batch query all statuses from database
    let statusMap = {};
    try {
      const statusResult = await pool.query(
        `SELECT receipt_id, status, COUNT(*) as count 
         FROM processed_items 
         WHERE receipt_id = ANY($1::text[])
         GROUP BY receipt_id, status`,
        [receiptIds]
      );
      
      for (const row of statusResult.rows) {
        if (!statusMap[row.receipt_id]) {
          statusMap[row.receipt_id] = { completed: 0, skipped: 0 };
        }
        if (row.status === 'completed') statusMap[row.receipt_id].completed = parseInt(row.count);
        if (row.status === 'skipped') statusMap[row.receipt_id].skipped = parseInt(row.count);
      }
    } catch (e) {
      console.error('Error batch querying statuses:', e);
    }
    
    // Process receipts
    const receipts = [];
    for (const r of data.results) {
      let vendorName = 'Unknown Vendor';
      let itemCount = 0;
      let gridCount = 0;
      
      // Get lines and extract vendor from first item
      try {
        const lines = await heartlandRequest(`/purchasing/receipts/${r.id}/lines?per_page=100`);
        itemCount = lines.total || 0;
        
        // Count unique grids
        const gridIds = new Set();
        for (const line of lines.results || []) {
          if (line.grid_id) {
            gridIds.add(line.grid_id);
          }
        }
        
        // Get vendor from first item's primary_vendor_id (and grid_id if not found in lines)
        if (lines.results && lines.results.length > 0) {
          const firstLine = lines.results[0];
          if (firstLine.item_id) {
            try {
              const item = await heartlandRequest(`/items/${firstLine.item_id}`);
              if (item.primary_vendor_id) {
                vendorName = await getVendorName(item.primary_vendor_id);
              }
              // If no grid_ids found in lines, get from item
              if (gridIds.size === 0 && item.grid_id) {
                gridIds.add(item.grid_id);
              }
            } catch (e) {
              console.error('Error fetching item for vendor:', e);
            }
          }
        }
        
        gridCount = gridIds.size || 1; // Default to 1 if still no grids found
      } catch (e) {
        console.error('Error fetching receipt lines:', e);
      }
      
      // Calculate receipt status from pre-fetched data
      let receiptStatus = 'new';
      const stats = statusMap[r.id.toString()];
      if (stats) {
        const done = stats.completed + stats.skipped;
        if (done > 0) {
          if (done >= gridCount) {
            receiptStatus = 'completed';
          } else {
            receiptStatus = 'in_progress';
          }
        }
      }
      
      receipts.push({
        id: `REC-${r.id}`,
        heartlandId: r.id,
        date: r.updated_at ? r.updated_at.split('T')[0] : (r.created_at ? r.created_at.split('T')[0] : new Date().toISOString().split('T')[0]),
        vendor: vendorName,
        receiptNumber: r.public_id || `${r.id}`,
        itemCount: itemCount,
        gridCount: gridCount,
        status: receiptStatus,
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
      longDescription: grid.long_description || grid.item_long_description || '',
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
    const { longDescription, status, receiptId } = req.body;
    
    // If there's a description, update Heartland
    if (longDescription !== undefined) {
      await heartlandRequest(`/items/${itemId}`, {
        method: 'PUT',
        body: JSON.stringify({
          long_description: longDescription,
        }),
      });
    }
    
    // Update status in database (include receiptId for status tracking)
    const newStatus = status || 'completed';
    const cleanReceiptId = receiptId ? receiptId.replace('REC-', '') : null;
    await updateItemStatus('item', itemId, newStatus, cleanReceiptId, req.username);
    
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
    const { longDescription, status, receiptId } = req.body;
    
    // If there's a description, update Heartland
    if (longDescription !== undefined) {
      await heartlandRequest(`/item_grids/${gridId}`, {
        method: 'PUT',
        body: JSON.stringify({
          long_description: longDescription,
        }),
      });
    }
    
    // Update status in database (include receiptId for status tracking)
    const newStatus = status || 'completed';
    const cleanReceiptId = receiptId ? receiptId.replace('REC-', '') : null;
    await updateItemStatus('grid', gridId, newStatus, cleanReceiptId, req.username);
    
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

// ==================== INVENTORY IQ ENDPOINTS ====================

// Get cached inventory data (fast read)
app.get('/api/inventory/data', async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT data, synced_at FROM inventory_cache WHERE cache_key = 'inventory_analysis'`
    );
    
    if (result.rows.length === 0) {
      return res.json({ 
        error: 'No data yet', 
        message: 'Run a sync first',
        syncedAt: null 
      });
    }
    
    const { data, synced_at } = result.rows[0];
    res.json({
      ...data,
      syncedAt: synced_at
    });
  } catch (error) {
    console.error('Error reading inventory cache:', error);
    res.status(500).json({ error: error.message });
  }
});

// Sync inventory data (heavy calculation - run nightly or manually)
app.post('/api/inventory/sync', async (req, res) => {
  // Set a long timeout for this endpoint
  req.setTimeout(120000); // 2 minutes
  
  try {
    console.log('Starting inventory sync...');
    const startTime = Date.now();
    
    // ========== STEP 1: Get receipts and build item receive dates ==========
    console.log('Fetching receipts...');
    const sixMonthsAgo = new Date(Date.now() - 180*24*60*60*1000).toISOString().split('T')[0];
    const receiptsData = await heartlandRequest(`/purchasing/receipts?per_page=100&_filter[created_at][$gte]=${sixMonthsAgo}&_filter[status]=complete`);
    console.log(`Found ${receiptsData.results?.length || 0} receipts`);
    
    // Map item_id -> { receiveDate, vendor_id }
    const itemReceiveInfo = {};
    let receiptCount = 0;
    for (const receipt of (receiptsData.results || []).slice(0, 75)) { // 75 receipts
      try {
        const lines = await heartlandRequest(`/purchasing/receipts/${receipt.id}/lines?per_page=75`);
        const receiveDate = receipt.completed_at || receipt.created_at;
        for (const line of lines.results || []) {
          if (line.item_id) {
            if (!itemReceiveInfo[line.item_id] || receiveDate < itemReceiveInfo[line.item_id].receiveDate) {
              itemReceiveInfo[line.item_id] = {
                receiveDate: receiveDate,
                category: line.item_custom?.category || line.item_custom?.Category || '',
                vendor: null
              };
            }
          }
        }
        receiptCount++;
        if (receiptCount % 25 === 0) {
          console.log(`Processed ${receiptCount} receipts...`);
        }
      } catch (e) {
        console.error(`Error fetching receipt ${receipt.id} lines:`, e.message);
      }
    }
    console.log(`Processed ${receiptCount} receipts, mapped ${Object.keys(itemReceiveInfo).length} items`);
    
    // ========== STEP 2: Get sales data ==========
    console.log('Fetching sales data...');
    const salesData = await heartlandRequest('/reporting/sales?per_page=750');
    console.log(`Found ${salesData.results?.length || 0} sales records`);
    
    // Map item_id -> array of sale records
    const itemSales = {};
    for (const sale of salesData.results || []) {
      if (sale.item_id && sale.net_qty_sold > 0) {
        if (!itemSales[sale.item_id]) {
          itemSales[sale.item_id] = [];
        }
        itemSales[sale.item_id].push({
          date: sale.datetime,
          qty: sale.net_qty_sold,
          revenue: sale.net_sales
        });
      }
    }
    
    // ========== STEP 3: Get current inventory ==========
    console.log('Fetching inventory...');
    const inventoryData = await heartlandRequest('/inventory/values?group[]=item_id&per_page=300');
    console.log(`Found ${inventoryData.results?.length || 0} inventory items`);
    
    // ========== STEP 4: Get item details for categorization ==========
    console.log('Fetching item details...');
    const itemDetails = {};
    
    // Prioritize items that are in inventory (more relevant)
    const inventoryItemIds = (inventoryData.results || [])
      .map(i => i.item_id?.toString())
      .filter(Boolean);
    
    let itemCount = 0;
    for (const itemId of inventoryItemIds) {
      if (itemCount >= 150) break; // 150 items
      try {
        const item = await heartlandRequest(`/items/${itemId}`);
        let vendorName = 'Unknown';
        if (item.primary_vendor_id) {
          vendorName = await getVendorName(item.primary_vendor_id);
        }
        itemDetails[itemId] = {
          name: item.custom?.style_name || item.description || 'Unknown',
          category: item.custom?.category || item.custom?.Category || 'Uncategorized',
          vendor: vendorName,
          cost: item.cost || 0,
          price: item.price || 0
        };
        itemCount++;
        if (itemCount % 20 === 0) {
          console.log(`Fetched ${itemCount} item details...`);
        }
      } catch (e) {
        // Skip items that can't be fetched
      }
    }
    console.log(`Fetched details for ${itemCount} items`);
    
    // ========== STEP 5: Calculate sell-through velocity ==========
    const now = new Date();
    const velocityByCategory = {};
    const velocityByVendor = {};
    
    // For each item that has both receive date and sales
    for (const [itemId, sales] of Object.entries(itemSales)) {
      const receiveInfo = itemReceiveInfo[itemId];
      const details = itemDetails[itemId];
      
      if (!receiveInfo || !details) continue;
      
      const receiveDate = new Date(receiveInfo.receiveDate);
      
      // Calculate days to first sale
      const firstSale = sales.sort((a, b) => new Date(a.date) - new Date(b.date))[0];
      const daysToSell = Math.floor((new Date(firstSale.date) - receiveDate) / (1000 * 60 * 60 * 24));
      
      if (daysToSell < 0 || daysToSell > 365) continue; // Skip bad data
      
      const category = details.category;
      const vendor = details.vendor;
      
      // Aggregate by category
      if (!velocityByCategory[category]) {
        velocityByCategory[category] = { totalDays: 0, count: 0, items: [] };
      }
      velocityByCategory[category].totalDays += daysToSell;
      velocityByCategory[category].count++;
      
      // Aggregate by vendor
      if (!velocityByVendor[vendor]) {
        velocityByVendor[vendor] = { totalDays: 0, count: 0, items: [] };
      }
      velocityByVendor[vendor].totalDays += daysToSell;
      velocityByVendor[vendor].count++;
    }
    
    // Calculate averages and format
    const categoryVelocity = Object.entries(velocityByCategory)
      .map(([name, data]) => ({
        name,
        avgDaysToSell: Math.round(data.totalDays / data.count),
        itemsSold: data.count
      }))
      .filter(c => c.itemsSold >= 3) // Only include categories with enough data
      .sort((a, b) => a.avgDaysToSell - b.avgDaysToSell);
    
    const vendorVelocity = Object.entries(velocityByVendor)
      .map(([name, data]) => ({
        name,
        avgDaysToSell: Math.round(data.totalDays / data.count),
        itemsSold: data.count
      }))
      .filter(v => v.itemsSold >= 3) // Only include vendors with enough data
      .sort((a, b) => a.avgDaysToSell - b.avgDaysToSell);
    
    // ========== STEP 6: Calculate dead stock ==========
    const deadStockItems = [];
    let total60Days = 0, total90Days = 0, total120Days = 0;
    let value60Days = 0, value90Days = 0, value120Days = 0;
    
    for (const inv of inventoryData.results || []) {
      if (!inv.item_id) continue;
      const qty = inv.qty_on_hand || 0;
      if (qty <= 0) continue;
      
      const details = itemDetails[inv.item_id];
      const receiveInfo = itemReceiveInfo[inv.item_id];
      const sales = itemSales[inv.item_id];
      
      if (!details) continue;
      
      // Calculate days stagnant
      let daysStagnant = 0;
      let lastSaleDate = null;
      
      if (sales && sales.length > 0) {
        const lastSale = sales.sort((a, b) => new Date(b.date) - new Date(a.date))[0];
        lastSaleDate = lastSale.date;
        daysStagnant = Math.floor((now - new Date(lastSaleDate)) / (1000 * 60 * 60 * 24));
      } else if (receiveInfo) {
        daysStagnant = Math.floor((now - new Date(receiveInfo.receiveDate)) / (1000 * 60 * 60 * 24));
      } else {
        continue;
      }
      
      if (daysStagnant < 60) continue;
      
      const cost = details.cost || inv.unit_cost || 0;
      const value = qty * cost;
      
      let suggestedMarkdown = '20% off';
      if (daysStagnant >= 120) {
        suggestedMarkdown = '40% off';
        total120Days++;
        value120Days += value;
      } else if (daysStagnant >= 90) {
        suggestedMarkdown = '30% off';
        total90Days++;
        value90Days += value;
      } else {
        total60Days++;
        value60Days += value;
      }
      
      deadStockItems.push({
        id: inv.item_id,
        name: details.name,
        category: details.category,
        vendor: details.vendor,
        qty: qty,
        price: details.price,
        value: Math.round(value * 100) / 100,
        daysStagnant: daysStagnant,
        lastSaleDate: lastSaleDate ? lastSaleDate.split('T')[0] : null,
        suggestedMarkdown: suggestedMarkdown
      });
    }
    
    deadStockItems.sort((a, b) => b.daysStagnant - a.daysStagnant);
    
    // ========== STEP 7: Save to cache ==========
    const analysisData = {
      deadStock: {
        summary: {
          items60Days: total60Days,
          items90Days: total90Days,
          items120Days: total120Days,
          value60Days: Math.round(value60Days * 100) / 100,
          value90Days: Math.round(value90Days * 100) / 100,
          value120Days: Math.round(value120Days * 100) / 100,
          totalItems: total60Days + total90Days + total120Days,
          totalValue: Math.round((value60Days + value90Days + value120Days) * 100) / 100,
        },
        items: deadStockItems.slice(0, 100) // Limit to top 100
      },
      velocity: {
        byCategory: categoryVelocity,
        byVendor: vendorVelocity
      },
      stats: {
        totalItemsAnalyzed: itemCount,
        totalReceipts: receiptsData.results?.length || 0,
        totalSalesRecords: salesData.results?.length || 0
      }
    };
    
    await pool.query(
      `INSERT INTO inventory_cache (cache_key, data, synced_at)
       VALUES ('inventory_analysis', $1, CURRENT_TIMESTAMP)
       ON CONFLICT (cache_key)
       DO UPDATE SET data = $1, synced_at = CURRENT_TIMESTAMP`,
      [JSON.stringify(analysisData)]
    );
    
    const duration = Math.round((Date.now() - startTime) / 1000);
    console.log(`Inventory sync complete in ${duration}s`);
    
    res.json({
      success: true,
      duration: `${duration}s`,
      stats: analysisData.stats
    });
  } catch (error) {
    console.error('Error syncing inventory:', error);
    res.status(500).json({ error: error.message });
  }
});

// ============================================================
// NIGHTLY SYNC SYSTEM - Sales, Customers, and Analytics
// ============================================================

// Sync secret key for cron job authentication
const SYNC_SECRET = process.env.SYNC_SECRET || 'boutiqueflow-sync-2024';

// Helper: Create sync log entry
async function createSyncLog(syncType) {
  const result = await pool.query(
    `INSERT INTO sync_log (sync_type, status) VALUES ($1, 'running') RETURNING id`,
    [syncType]
  );
  return result.rows[0].id;
}

// Helper: Update sync log
async function updateSyncLog(logId, status, recordsProcessed, errorMessage = null) {
  await pool.query(
    `UPDATE sync_log 
     SET status = $1, records_processed = $2, error_message = $3, 
         completed_at = CURRENT_TIMESTAMP,
         duration_seconds = EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - started_at))::INTEGER
     WHERE id = $4`,
    [status, recordsProcessed, errorMessage, logId]
  );
}

// Helper: Fetch all pages from Heartland API
async function fetchAllPages(endpoint, maxPages = 50) {
  const allResults = [];
  let page = 1;
  let hasMore = true;
  
  while (hasMore && page <= maxPages) {
    const separator = endpoint.includes('?') ? '&' : '?';
    const data = await heartlandRequest(`${endpoint}${separator}page=${page}&per_page=100`);
    
    if (data.results && data.results.length > 0) {
      allResults.push(...data.results);
      hasMore = data.results.length === 100; // If we got 100, there might be more
      page++;
    } else {
      hasMore = false;
    }
  }
  
  return allResults;
}

// MAIN NIGHTLY SYNC - Fetches sales transactions and builds customer profiles
app.post('/api/sync/nightly', async (req, res) => {
  // Verify sync secret (for cron job security)
  const providedKey = req.query.key || req.body.key;
  if (providedKey !== SYNC_SECRET) {
    return res.status(401).json({ error: 'Invalid sync key' });
  }
  
  // Don't wait for sync to complete - return immediately
  res.json({ message: 'Nightly sync started', status: 'running' });
  
  // Run sync in background
  runNightlySync().catch(err => {
    console.error('Nightly sync failed:', err);
  });
});

// The actual sync logic (can be called from endpoint or worker)
async function runNightlySync() {
  const logId = await createSyncLog('nightly_full');
  const startTime = Date.now();
  let totalRecords = 0;
  
  try {
    console.log('========== NIGHTLY SYNC STARTED ==========');
    
    // ========== STEP 1: Fetch Sales from Reporting API ==========
    console.log('Step 1: Fetching sales data from reporting API...');
    const oneYearAgo = new Date(Date.now() - 365*24*60*60*1000).toISOString().split('T')[0];
    
    // Get all sales from the past year using reporting/sales (has all the data we need)
    const sales = await fetchAllPages(`/reporting/sales?_filter[date][$gte]=${oneYearAgo}`);
    console.log(`Found ${sales.length} sales records`);
    
    // ========== STEP 2: Process Sales Records ==========
    console.log('Step 2: Processing sales records...');
    let transactionCount = 0;
    
    // Get item details for category/brand info (batch by unique item IDs)
    const uniqueItemIds = [...new Set(sales.map(s => s.item_id).filter(Boolean))];
    console.log(`Fetching details for ${uniqueItemIds.length} unique items...`);
    
    const itemDetailsCache = {};
    let itemsFetched = 0;
    for (const itemId of uniqueItemIds) {
      try {
        const item = await heartlandRequest(`/items/${itemId}`);
        let vendorName = 'Unknown';
        if (item.primary_vendor_id) {
          vendorName = await getVendorName(item.primary_vendor_id);
        }
        itemDetailsCache[itemId] = {
          category: item.custom?.category || item.custom?.Category || 'Uncategorized',
          brand: item.custom?.brand || item.custom?.Brand || vendorName,
          vendor: vendorName,
          name: item.description || 'Unknown Item',
          size: item.custom?.size || item.custom?.Size || '',
          color: item.custom?.color_name || item.custom?.Color_Name || ''
        };
        itemsFetched++;
        if (itemsFetched % 100 === 0) {
          console.log(`Fetched ${itemsFetched}/${uniqueItemIds.length} item details...`);
        }
      } catch (e) {
        // Item might be deleted, use defaults
        itemDetailsCache[itemId] = {
          category: 'Uncategorized',
          brand: 'Unknown',
          vendor: 'Unknown',
          name: 'Unknown Item',
          size: '',
          color: ''
        };
      }
    }
    console.log(`Fetched details for ${itemsFetched} items`);
    
    // Now insert all sales records
    console.log('Inserting sales transactions...');
    for (const sale of sales) {
      if (!sale.item_id) continue;
      
      const itemDetails = itemDetailsCache[sale.item_id] || {};
      const transactionDate = new Date(sale.datetime);
      
      try {
        // Upsert transaction using reporting sales data
        await pool.query(`
          INSERT INTO sales_transactions 
            (heartland_ticket_id, heartland_line_id, customer_id, item_id, 
             transaction_date, day_of_week, hour_of_day, quantity, unit_price, 
             total_amount, category, vendor, brand, item_name, item_size, item_color)
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
          ON CONFLICT (heartland_ticket_id, heartland_line_id)
          DO UPDATE SET
            customer_id = EXCLUDED.customer_id,
            quantity = EXCLUDED.quantity,
            unit_price = EXCLUDED.unit_price,
            total_amount = EXCLUDED.total_amount
        `, [
          sale.transaction_id.toString(),
          sale.transaction_line_id?.toString() || '0',
          sale.customer_id?.toString() || null,
          sale.item_id?.toString(),
          transactionDate,
          transactionDate.getDay(), // 0=Sunday
          sale.hour || transactionDate.getHours(),
          sale.net_qty_sold || 1,
          sale.unit_price || 0,
          sale.net_sales || 0,
          itemDetails.category || 'Uncategorized',
          itemDetails.vendor || 'Unknown',
          itemDetails.brand || 'Unknown',
          itemDetails.name || 'Unknown Item',
          itemDetails.size || '',
          itemDetails.color || ''
        ]);
        
        transactionCount++;
        if (transactionCount % 500 === 0) {
          console.log(`Inserted ${transactionCount}/${sales.length} transactions...`);
        }
      } catch (e) {
        // Skip duplicates or errors
        if (!e.message.includes('duplicate')) {
          console.error(`Error inserting sale ${sale.id}:`, e.message);
        }
      }
    }
    
    console.log(`Inserted/updated ${transactionCount} transactions`);
    totalRecords += transactionCount;
    
    // ========== STEP 3: Build Customer Profiles ==========
    console.log('Step 3: Building customer profiles...');
    
    // Get all customers from Heartland
    const heartlandCustomers = await fetchAllPages('/customers');
    console.log(`Found ${heartlandCustomers.length} customers in Heartland`);
    
    // Process each customer with aggregated purchase data
    let customerCount = 0;
    for (const cust of heartlandCustomers) {
      if (!cust.id) continue;
      
      // Get aggregated stats from our transactions
      const statsResult = await pool.query(`
        SELECT 
          COUNT(*) as total_purchases,
          SUM(total_amount) as lifetime_value,
          AVG(total_amount) as avg_purchase,
          MIN(transaction_date) as first_purchase,
          MAX(transaction_date) as last_purchase
        FROM sales_transactions
        WHERE customer_id = $1
      `, [cust.id.toString()]);
      
      const stats = statsResult.rows[0];
      
      // Get preferred brands (top 5)
      const brandsResult = await pool.query(`
        SELECT brand, COUNT(*) as cnt
        FROM sales_transactions
        WHERE customer_id = $1 AND brand IS NOT NULL AND brand != ''
        GROUP BY brand
        ORDER BY cnt DESC
        LIMIT 5
      `, [cust.id.toString()]);
      
      // Get sizes purchased
      const sizesResult = await pool.query(`
        SELECT DISTINCT item_size
        FROM sales_transactions
        WHERE customer_id = $1 AND item_size IS NOT NULL AND item_size != ''
      `, [cust.id.toString()]);
      
      // Get preferred categories (top 5)
      const categoriesResult = await pool.query(`
        SELECT category, COUNT(*) as cnt
        FROM sales_transactions
        WHERE customer_id = $1 AND category IS NOT NULL AND category != 'Uncategorized'
        GROUP BY category
        ORDER BY cnt DESC
        LIMIT 5
      `, [cust.id.toString()]);
      
      // Upsert customer profile
      await pool.query(`
        INSERT INTO customers 
          (heartland_customer_id, first_name, last_name, email, phone,
           total_purchases, lifetime_value, first_purchase_date, last_purchase_date,
           avg_purchase_value, preferred_brands, preferred_sizes, preferred_categories)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
        ON CONFLICT (heartland_customer_id)
        DO UPDATE SET
          first_name = EXCLUDED.first_name,
          last_name = EXCLUDED.last_name,
          email = EXCLUDED.email,
          phone = EXCLUDED.phone,
          total_purchases = EXCLUDED.total_purchases,
          lifetime_value = EXCLUDED.lifetime_value,
          first_purchase_date = EXCLUDED.first_purchase_date,
          last_purchase_date = EXCLUDED.last_purchase_date,
          avg_purchase_value = EXCLUDED.avg_purchase_value,
          preferred_brands = EXCLUDED.preferred_brands,
          preferred_sizes = EXCLUDED.preferred_sizes,
          preferred_categories = EXCLUDED.preferred_categories,
          updated_at = CURRENT_TIMESTAMP
      `, [
        cust.id.toString(),
        cust.first_name || '',
        cust.last_name || '',
        cust.email || cust.emails?.[0]?.address || null,
        cust.phone || cust.phones?.[0]?.number || null,
        parseInt(stats.total_purchases) || 0,
        parseFloat(stats.lifetime_value) || 0,
        stats.first_purchase || null,
        stats.last_purchase || null,
        parseFloat(stats.avg_purchase) || 0,
        JSON.stringify(brandsResult.rows.map(r => r.brand)),
        JSON.stringify(sizesResult.rows.map(r => r.item_size)),
        JSON.stringify(categoriesResult.rows.map(r => r.category))
      ]);
      
      customerCount++;
      if (customerCount % 100 === 0) {
        console.log(`Processed ${customerCount}/${heartlandCustomers.length} customers...`);
      }
    }
    
    console.log(`Updated ${customerCount} customer profiles`);
    totalRecords += customerCount;
    
    // ========== STEP 4: Aggregate SalesIQ Data ==========
    console.log('Step 4: Aggregating SalesIQ data...');
    
    // Sales by day of week
    const dayOfWeekResult = await pool.query(`
      SELECT 
        day_of_week,
        COUNT(DISTINCT heartland_ticket_id) as transactions,
        SUM(total_amount) as revenue
      FROM sales_transactions
      WHERE transaction_date >= NOW() - INTERVAL '90 days'
      GROUP BY day_of_week
      ORDER BY day_of_week
    `);
    
    // Sales by hour
    const hourlyResult = await pool.query(`
      SELECT 
        hour_of_day,
        AVG(total_amount) as avg_sale,
        COUNT(*) as transaction_count
      FROM sales_transactions
      WHERE transaction_date >= NOW() - INTERVAL '90 days'
      GROUP BY hour_of_day
      ORDER BY hour_of_day
    `);
    
    // Day/Hour heatmap
    const heatmapResult = await pool.query(`
      SELECT 
        day_of_week,
        hour_of_day,
        SUM(total_amount) as revenue
      FROM sales_transactions
      WHERE transaction_date >= NOW() - INTERVAL '90 days'
      GROUP BY day_of_week, hour_of_day
      ORDER BY day_of_week, hour_of_day
    `);
    
    // Category performance
    const categoryResult = await pool.query(`
      SELECT 
        category,
        SUM(total_amount) as revenue,
        COUNT(*) as transactions
      FROM sales_transactions
      WHERE transaction_date >= NOW() - INTERVAL '90 days'
        AND category != 'Uncategorized'
      GROUP BY category
      ORDER BY revenue DESC
      LIMIT 10
    `);
    
    // Build the cache data
    const dayNames = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
    const salesData = {
      dailySales: dayNames.map((day, idx) => {
        const data = dayOfWeekResult.rows.find(r => r.day_of_week === idx);
        return {
          day,
          revenue: Math.round(parseFloat(data?.revenue || 0)),
          transactions: parseInt(data?.transactions || 0)
        };
      }),
      hourlyAvg: Array.from({length: 24}, (_, hour) => {
        const data = hourlyResult.rows.find(r => r.hour_of_day === hour);
        return {
          hour: `${hour % 12 || 12}${hour < 12 ? 'am' : 'pm'}`,
          avg: Math.round(parseFloat(data?.avg_sale || 0))
        };
      }).filter(h => h.avg > 0), // Only include hours with sales
      heatmap: {
        days: dayNames,
        hours: Array.from({length: 12}, (_, i) => `${(i + 10) % 12 || 12}${(i + 10) < 12 ? 'am' : 'pm'}`), // 10am-9pm typical retail
        values: dayNames.map((_, dayIdx) => {
          return Array.from({length: 12}, (_, hourOffset) => {
            const hour = hourOffset + 10; // Start at 10am
            const data = heatmapResult.rows.find(r => r.day_of_week === dayIdx && r.hour_of_day === hour);
            return Math.round(parseFloat(data?.revenue || 0));
          });
        })
      },
      categories: categoryResult.rows.map(r => ({
        name: r.category,
        revenue: Math.round(parseFloat(r.revenue)),
        transactions: parseInt(r.transactions)
      }))
    };
    
    // Save to cache
    await pool.query(`
      INSERT INTO sales_cache (cache_key, data, synced_at)
      VALUES ('sales_analysis', $1, CURRENT_TIMESTAMP)
      ON CONFLICT (cache_key)
      DO UPDATE SET data = $1, synced_at = CURRENT_TIMESTAMP
    `, [JSON.stringify(salesData)]);
    
    console.log('SalesIQ data cached');
    
    // ========== STEP 5: Complete ==========
    const duration = Math.round((Date.now() - startTime) / 1000);
    console.log(`========== NIGHTLY SYNC COMPLETE in ${duration}s ==========`);
    console.log(`Total records processed: ${totalRecords}`);
    
    await updateSyncLog(logId, 'completed', totalRecords);
    
    return { success: true, duration, totalRecords };
    
  } catch (error) {
    console.error('Nightly sync error:', error);
    await updateSyncLog(logId, 'failed', totalRecords, error.message);
    throw error;
  }
}

// Get SalesIQ data (reads from cache)
app.get('/api/sales/analysis', async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT data, synced_at FROM sales_cache WHERE cache_key = 'sales_analysis'`
    );
    
    if (result.rows.length === 0) {
      return res.json({
        error: 'No sales data available',
        message: 'Please run a sync first',
        syncedAt: null
      });
    }
    
    const { data, synced_at } = result.rows[0];
    res.json({
      ...data,
      syncedAt: synced_at
    });
  } catch (error) {
    console.error('Error reading sales cache:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get lapsed customers for Retargeting
app.get('/api/customers/lapsed', async (req, res) => {
  try {
    const days = parseInt(req.query.days) || 90;
    const limit = parseInt(req.query.limit) || 50;
    
    const result = await pool.query(`
      SELECT 
        heartland_customer_id,
        first_name,
        last_name,
        email,
        phone,
        total_purchases,
        lifetime_value,
        last_purchase_date,
        EXTRACT(DAY FROM NOW() - last_purchase_date)::INTEGER as days_since_purchase
      FROM customers
      WHERE last_purchase_date IS NOT NULL
        AND last_purchase_date < NOW() - INTERVAL '1 day' * $1
        AND (email IS NOT NULL OR phone IS NOT NULL)
      ORDER BY lifetime_value DESC
      LIMIT $2
    `, [days, limit]);
    
    // Get last purchase details for each customer
    const customers = [];
    for (const cust of result.rows) {
      const lastPurchaseResult = await pool.query(`
        SELECT item_name, brand, item_color, item_size, total_amount, transaction_date
        FROM sales_transactions
        WHERE customer_id = $1
        ORDER BY transaction_date DESC
        LIMIT 1
      `, [cust.heartland_customer_id]);
      
      const lastPurchase = lastPurchaseResult.rows[0];
      
      customers.push({
        id: cust.heartland_customer_id,
        name: `${cust.first_name || ''} ${cust.last_name || ''}`.trim() || 'Unknown Customer',
        email: cust.email,
        phone: cust.phone,
        totalPurchases: cust.total_purchases,
        lifetimeValue: parseFloat(cust.lifetime_value),
        daysSincePurchase: cust.days_since_purchase,
        lastPurchase: lastPurchase ? {
          item: lastPurchase.item_name,
          brand: lastPurchase.brand,
          color: lastPurchase.item_color,
          size: lastPurchase.item_size,
          price: parseFloat(lastPurchase.total_amount),
          date: lastPurchase.transaction_date
        } : null
      });
    }
    
    // Get total lapsed count for stats
    const countResult = await pool.query(`
      SELECT COUNT(*) as total
      FROM customers
      WHERE last_purchase_date IS NOT NULL
        AND last_purchase_date < NOW() - INTERVAL '1 day' * $1
        AND (email IS NOT NULL OR phone IS NOT NULL)
    `, [days]);
    
    res.json({
      customers,
      total: parseInt(countResult.rows[0].total),
      threshold: days
    });
  } catch (error) {
    console.error('Error fetching lapsed customers:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get customer matches for First Dibs (based on new inventory)
app.get('/api/customers/match', async (req, res) => {
  try {
    const { brand, category, size } = req.query;
    const limit = parseInt(req.query.limit) || 20;
    
    if (!brand && !category) {
      return res.status(400).json({ error: 'At least brand or category required' });
    }
    
    // Build query to find customers who have purchased similar items
    let query = `
      SELECT DISTINCT ON (c.heartland_customer_id)
        c.heartland_customer_id,
        c.first_name,
        c.last_name,
        c.email,
        c.phone,
        c.total_purchases,
        c.lifetime_value,
        c.last_purchase_date,
        c.preferred_brands,
        c.preferred_sizes,
        EXTRACT(DAY FROM NOW() - c.last_purchase_date)::INTEGER as days_since_purchase
      FROM customers c
      INNER JOIN sales_transactions st ON st.customer_id = c.heartland_customer_id
      WHERE (c.email IS NOT NULL OR c.phone IS NOT NULL)
    `;
    
    const params = [];
    let paramIndex = 1;
    
    if (brand) {
      query += ` AND (st.brand = $${paramIndex} OR c.preferred_brands LIKE $${paramIndex + 1})`;
      params.push(brand, `%${brand}%`);
      paramIndex += 2;
    }
    
    if (category) {
      query += ` AND (st.category = $${paramIndex} OR c.preferred_categories LIKE $${paramIndex + 1})`;
      params.push(category, `%${category}%`);
      paramIndex += 2;
    }
    
    if (size) {
      query += ` AND (st.item_size = $${paramIndex} OR c.preferred_sizes LIKE $${paramIndex + 1})`;
      params.push(size, `%${size}%`);
      paramIndex += 2;
    }
    
    query += ` ORDER BY c.heartland_customer_id, c.lifetime_value DESC LIMIT $${paramIndex}`;
    params.push(limit);
    
    const result = await pool.query(query, params);
    
    // Get previous purchase of matched item for each customer
    const customers = [];
    for (const cust of result.rows) {
      let previousPurchaseQuery = `
        SELECT item_name, brand, item_color, item_size, total_amount, transaction_date
        FROM sales_transactions
        WHERE customer_id = $1
      `;
      const purchaseParams = [cust.heartland_customer_id];
      
      if (brand) {
        previousPurchaseQuery += ` AND brand = $2`;
        purchaseParams.push(brand);
      }
      
      previousPurchaseQuery += ` ORDER BY transaction_date DESC LIMIT 1`;
      
      const prevPurchaseResult = await pool.query(previousPurchaseQuery, purchaseParams);
      const prevPurchase = prevPurchaseResult.rows[0];
      
      customers.push({
        id: cust.heartland_customer_id,
        name: `${cust.first_name || ''} ${cust.last_name || ''}`.trim() || 'Unknown Customer',
        email: cust.email,
        phone: cust.phone,
        daysSincePurchase: cust.days_since_purchase,
        previousPurchase: prevPurchase ? {
          item: prevPurchase.item_name,
          brand: prevPurchase.brand,
          color: prevPurchase.item_color,
          size: prevPurchase.item_size,
          price: parseFloat(prevPurchase.total_amount),
          date: prevPurchase.transaction_date
        } : null
      });
    }
    
    res.json({ customers });
  } catch (error) {
    console.error('Error matching customers:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get First Dibs matches - new arrivals matched to customers
app.get('/api/first-dibs/matches', async (req, res) => {
  try {
    const limit = parseInt(req.query.limit) || 20;
    const daysBack = parseInt(req.query.days) || 7; // Look at receipts from last 7 days
    
    // Step 1: Get recent receipt items (new arrivals)
    const dateFilter = new Date(Date.now() - daysBack * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
    
    const receipts = await heartlandRequest(
      `/purchasing/receipts?per_page=20&_filter[created_at][$gte]=${dateFilter}&_filter[status]=complete&sort[]=updated_at,desc`
    );
    
    if (!receipts.results || receipts.results.length === 0) {
      return res.json({ 
        matches: [], 
        newItemCount: 0,
        message: 'No recent receipts found' 
      });
    }
    
    // Step 2: Get unique items from recent receipts with their details
    const newItems = [];
    const seenGrids = new Set();
    
    for (const receipt of receipts.results.slice(0, 10)) { // Limit to 10 most recent receipts
      try {
        const lines = await heartlandRequest(`/purchasing/receipts/${receipt.id}/lines?per_page=50`);
        
        for (const line of (lines.results || []).slice(0, 20)) {
          if (!line.item_id) continue;
          
          try {
            const item = await heartlandRequest(`/items/${line.item_id}`);
            
            // Skip if we've already processed this grid
            if (item.grid_id && seenGrids.has(item.grid_id)) continue;
            if (item.grid_id) seenGrids.add(item.grid_id);
            
            const brand = item.custom?.brand || item.custom?.Brand || '';
            const category = item.custom?.category || item.custom?.Category || '';
            const size = item.custom?.size || item.custom?.Size || '';
            
            // Only include items with brand info (needed for matching)
            if (!brand) continue;
            
            let vendorName = brand;
            if (item.primary_vendor_id) {
              vendorName = await getVendorName(item.primary_vendor_id);
            }
            
            newItems.push({
              id: item.grid_id || item.id,
              name: item.custom?.style_name || item.description || 'Unknown Item',
              brand: vendorName,
              category: category,
              color: item.custom?.color_name || item.custom?.Color_Name || '',
              size: size,
              price: item.price || 0,
              receiptDate: receipt.created_at,
              itemId: item.id
            });
            
            if (newItems.length >= 30) break; // Limit items to process
          } catch (e) {
            // Skip items that can't be fetched
          }
        }
        
        if (newItems.length >= 30) break;
      } catch (e) {
        console.error(`Error fetching receipt ${receipt.id} lines:`, e.message);
      }
    }
    
    // Step 3: For each new item, find matching customers
    const matches = [];
    
    for (const item of newItems) {
      // Find customers who have bought this brand before
      const customerResult = await pool.query(`
        SELECT DISTINCT ON (c.heartland_customer_id)
          c.heartland_customer_id,
          c.first_name,
          c.last_name,
          c.email,
          c.phone,
          c.lifetime_value,
          EXTRACT(DAY FROM NOW() - c.last_purchase_date)::INTEGER as days_since_purchase
        FROM customers c
        INNER JOIN sales_transactions st ON st.customer_id = c.heartland_customer_id
        WHERE (c.email IS NOT NULL OR c.phone IS NOT NULL)
          AND st.brand = $1
        ORDER BY c.heartland_customer_id, c.lifetime_value DESC
        LIMIT 5
      `, [item.brand]);
      
      for (const cust of customerResult.rows) {
        // Get customer's previous purchase of this brand
        const prevPurchaseResult = await pool.query(`
          SELECT item_name, brand, item_color, item_size, total_amount, transaction_date
          FROM sales_transactions
          WHERE customer_id = $1 AND brand = $2
          ORDER BY transaction_date DESC
          LIMIT 1
        `, [cust.heartland_customer_id, item.brand]);
        
        const prevPurchase = prevPurchaseResult.rows[0];
        if (!prevPurchase) continue;
        
        // Calculate match strength
        let matchStrength = 'good';
        const sizeMatch = prevPurchase.item_size === item.size;
        if (sizeMatch && cust.days_since_purchase < 90) {
          matchStrength = 'strong';
        }
        
        // Generate draft message
        const firstName = cust.first_name || 'there';
        const draftMessage = `Hi ${firstName}! It's Kelly at The Boutique. We just got the new ${item.brand} ${item.name} and I immediately thought of you based on your love of ${item.brand}. Want me to hold one for you? 💙`;
        
        matches.push({
          id: `${item.id}-${cust.heartland_customer_id}`,
          customer: {
            id: cust.heartland_customer_id,
            name: `${cust.first_name || ''} ${cust.last_name || ''}`.trim() || 'Unknown Customer',
            email: cust.email,
            phone: cust.phone
          },
          previousPurchase: {
            item: prevPurchase.item_name,
            brand: prevPurchase.brand,
            color: prevPurchase.item_color,
            size: prevPurchase.item_size,
            date: prevPurchase.transaction_date ? new Date(prevPurchase.transaction_date).toISOString().split('T')[0] : null,
            price: parseFloat(prevPurchase.total_amount)
          },
          newItem: {
            name: item.name,
            brand: item.brand,
            color: item.color,
            size: item.size,
            price: item.price,
            imageUrl: null // Would need Shopify integration for images
          },
          daysSincePurchase: cust.days_since_purchase,
          daysSinceContact: null,
          draftMessage: draftMessage,
          matchStrength: matchStrength,
          itemStatus: 'ready'
        });
        
        if (matches.length >= limit) break;
      }
      
      if (matches.length >= limit) break;
    }
    
    // Sort by match strength (strong first) then by days since purchase
    matches.sort((a, b) => {
      if (a.matchStrength === 'strong' && b.matchStrength !== 'strong') return -1;
      if (b.matchStrength === 'strong' && a.matchStrength !== 'strong') return 1;
      return (a.daysSincePurchase || 999) - (b.daysSincePurchase || 999);
    });
    
    res.json({
      matches: matches.slice(0, limit),
      newItemCount: newItems.length,
      receiptCount: receipts.results.length
    });
    
  } catch (error) {
    console.error('Error getting First Dibs matches:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get sync status
app.get('/api/sync/status', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT * FROM sync_log 
      ORDER BY started_at DESC 
      LIMIT 10
    `);
    
    // Get data freshness
    const salesCache = await pool.query(
      `SELECT synced_at FROM sales_cache WHERE cache_key = 'sales_analysis'`
    );
    const inventoryCache = await pool.query(
      `SELECT synced_at FROM inventory_cache WHERE cache_key = 'inventory_analysis'`
    );
    
    // Get record counts
    const counts = await pool.query(`
      SELECT 
        (SELECT COUNT(*) FROM sales_transactions) as transactions,
        (SELECT COUNT(*) FROM customers) as customers
    `);
    
    res.json({
      recentSyncs: result.rows,
      dataFreshness: {
        sales: salesCache.rows[0]?.synced_at || null,
        inventory: inventoryCache.rows[0]?.synced_at || null
      },
      recordCounts: counts.rows[0]
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Manual sync trigger (for testing - requires auth)
app.post('/api/sync/manual', async (req, res) => {
  try {
    res.json({ message: 'Manual sync started', status: 'running' });
    
    // Run sync in background
    runNightlySync().catch(err => {
      console.error('Manual sync failed:', err);
    });
  } catch (error) {
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

// Debug endpoint to see processed items in database
app.get('/api/debug/processed-items', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM processed_items ORDER BY updated_at DESC LIMIT 50');
    res.json(result.rows);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Debug endpoint to see raw item data from Heartland
app.get('/api/debug/item/:id', async (req, res) => {
  try {
    const item = await heartlandRequest(`/items/${req.params.id}`);
    res.json(item);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Debug endpoint to see raw receipt line data from Heartland
app.get('/api/debug/receipt/:id', async (req, res) => {
  try {
    const receipt = await heartlandRequest(`/purchasing/receipts/${req.params.id}`);
    const lines = await heartlandRequest(`/purchasing/receipts/${req.params.id}/lines?per_page=10`);
    res.json({ receipt, lines });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Debug endpoint to check sales/reporting API
app.get('/api/debug/sales', async (req, res) => {
  try {
    // Try multiple endpoints to see what works
    const results = {};
    
    // Try 1: /sales/tickets
    try {
      const tickets = await heartlandRequest(`/sales/tickets?per_page=3`);
      results.tickets = tickets;
    } catch (e) {
      results.tickets_error = e.message;
    }
    
    // Try 2: /sales/transactions  
    try {
      const transactions = await heartlandRequest(`/sales/transactions?per_page=3`);
      results.transactions = transactions;
    } catch (e) {
      results.transactions_error = e.message;
    }
    
    // Try 3: /reporting/sales
    try {
      const reportingSales = await heartlandRequest(`/reporting/sales?per_page=3`);
      results.reporting_sales = reportingSales;
    } catch (e) {
      results.reporting_sales_error = e.message;
    }
    
    res.json(results);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Debug endpoint to check sales for a specific item
app.get('/api/debug/item-sales/:itemId', async (req, res) => {
  try {
    // Get sales for specific item
    const sales = await heartlandRequest(`/reporting/analyzer?metrics[]=item_qty_sold&metrics[]=item_total&group[]=item_id&filter[item_id]=${req.params.itemId}&per_page=10`);
    res.json({ sales });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`BoutiqueFlow running at http://localhost:${PORT}`);
  console.log(`Heartland API: ${HEARTLAND_BASE_URL}`);
});
